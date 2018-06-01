#!/usr/bin/env python

# ========================================================================
#  Copyright 2018 Joost van Griethuysen <j.v.griethuysen@nki.nl>
#
#  Licensed under the 3-clause BSD License
# ========================================================================

import argparse
import base64
import datetime
import json
import os
import logging
import logging.handlers
import time
import re

import six
from six.moves.urllib.request import Request, urlopen
from six.moves.urllib.error import HTTPError, URLError

LAST_CHANGE = 0
DEBUG_MODE = False
MAX_TRIES = 5
CONFIG_FILE = None

Logger = logging.getLogger('orthanc_forward.events')
UIDlogger = logging.getLogger('orthanc_forward.UIDs')

pacscrawlerdb = None

class OrthancInterface:
  """
  Interface class to handle RestAPI calls to the Orthanc server
  """

  def __init__(self, URL, user=None, password=None):
    self.URL = URL
    self.user = user
    self.password = password
    self.logger = logging.getLogger('orthanc_forward.events.interface')

  def getResponse(self, cmd):
    req = Request('%s%s' % (self.URL, cmd))
    req.headers = {}
    if self.user is not None and self.password is not None:
      req.headers['Authorization'] = 'Basic ' + base64.b64encode(six.b('%s:%s' % (self.user, self.password))).decode("utf-8")

    try:
      resp = urlopen(req)
      studies = json.loads(resp.read().decode("utf-8"))
      resp.close()
      return studies
    except HTTPError as e:
      self.logger.error('GET error! ' + e.read())  # Do not include a traceback, that's not interesting here (it's not a bug, but Orthanc complaining...)
      raise

  def postResponse(self, cmd, data):
    req = Request('%s%s' % (self.URL, cmd), json.dumps(data))
    req.headers = {'content-type': 'application/json'}
    if self.user is not None and self.password is not None:
      req.headers['Authorization'] = 'Basic ' + base64.b64encode('%s:%s' % (self.user, self.password))

    try:
      resp = urlopen(req)
      studies = json.loads(resp.read())
      resp.close()
      return studies
    except HTTPError as e:
      self.logger.error('POST error! ' + e.read())  # Do not include a traceback, that's not interesting here (it's not a bug, but Orthanc complaining...)
      raise


class PacsCrawlerInterface:

  def __init__(self, **config):
    from pacscrawler import data_connection

    self.logger = logging.getLogger('orthanc_forward.events.pacscrawler')
    self.config = config
    self.data_connection = data_connection

    self.connection = None
    self.queries = None

    self.required_queries = {"get_identity_patient",
                             "get_identity_study",
                             "get_identity_series",
                             "get_patient",
                             "get_study",
                             "get_series",
                             "insert_patient",
                             "insert_patientid",
                             "insert_study",
                             "insert_study_keys",
                             "insert_series",
                             "insert_series_keys"}

  def _init_connection(self):
    db_connectiontype = self.config['Database_Connection'].get('Type', 'N/A')

    self.connection = self.data_connection.getConnection(db_connectiontype, self.config.get('Database_Connection', {}))

    if self.connection is None:
      return 1

    self.queries = self.connection.queries

    if len(self.required_queries - set(self.queries.keys())) > 0:
      self.logger.error('Missing required queries %s in database connection type %s, exiting',
                        list(self.required_queries - set(self.queries.keys())), db_connectiontype)
      return 2

    return 0

  def process_new_series(self, change, series, interface):
    try:
      if self._init_connection() > 0:
        return

      self.logger.info('forwarding new series (Change %i) to pacscrawler database...', change['Seq'])

      study = interface.getResponse('/studies/%s' % series['ParentStudy'])
      patient = interface.getResponse('/patients/%s' % study['ParentPatient'])

      seriesTags = series.get('MainDicomTags', {})
      studyTags = study.get('MainDicomTags', {})
      patientTags = patient.get('MainDicomTags', {})

      with self.connection:
        pc_patient = self._get_patient(patientTags, change)
        if pc_patient is None:
          self.connection.rollback()
          return

        pc_study = self._get_study(pc_patient, studyTags, change)
        if pc_study is None:
          self.connection.rollback()
          return

        pc_series = self._get_series(pc_study, seriesTags, change)
        if pc_series is None:
          self.connection.rollback()
          return

    except Exception:
      self.logger.error('Error forwarding new series (Change %i) to pacscrawler database!', change['Seq'], exc_info=True)
    finally:
      if self.connection is not None:
        self.connection.close()
        self.connection = None

  def _get_patient(self, patientTags, change):
    patientID = patientTags.get('PatientID', None)
    if patientID is None or patientID == '':
      self.logger.error('Change %i: Could not find PatientID', change['Seq'])
      return None

    with self.connection.cursor() as c:
      c.execute(self.queries['get_patient'], (patientID,))
      patient = c.fetchone()

      if patient is not None:
        patient = patient[0]
      else:
        patientName = self._filter_nonASCII(patientTags.get('', None))
        patientGender = patientTags.get('', None)
        if patientGender == 'F':
          patientGender = 'V'
        patientDoB = patientTags.get('', None)
        if patientDoB == '':
          patientDoB = None
        if patientDoB is not None:
          patientDoB = datetime.datetime.strptime(patientDoB, '%Y%m%d').date()
        c.execute(self.queries['insert_patient'], (patientName, patientDoB, patientGender))
        c.execute(self.queries['get_identity_patient'])
        patient = c.fetchone()[0]  # Get the Database ID for this patient
        self.logger.debug('Added new patient (DB ID %s)', patient)

        c.execute(self.queries['insert_patientid'], (patientID, patient, None))

      return patient

  def _get_study(self, patient, studyTags, change):
    studyUID = studyTags.get('StudyInstanceUID', None)
    if studyUID is None or studyUID == '':
      self.logger.error('Change %i: Could not find StudyUID', change['Seq'])
      return None

    with self.connection.cursor() as c:
      c.execute(self.queries['get_study'], (studyUID,))
      study = c.fetchone()
      if study is not None:
        study = study[0]
      else:
        studyDate = studyTags.get('StudyDate', None)
        if studyDate == '':
          studyDate = None
        elif studyDate is not None:
          studyDate = datetime.datetime.strptime(studyDate, '%Y%m%d').date()
        studyDescription = studyTags.get('StudyDescription', None)
        referringPhysician = studyTags.get('ReferringPhysicianName', None)
        institution = studyTags.get('InstitutionName', None)

        accessionNumber = studyTags.get('AccessionNumber', None)

        c.execute(self.queries['insert_study'], (patient,
                                                 studyDate,
                                                 None,
                                                 None,
                                                 studyDescription,
                                                 institution,
                                                 referringPhysician))
        c.execute(self.queries['get_identity_study'])
        study = c.fetchone()[0]

        self.logger.debug('Added new study (DB ID %s)', study)

        c.execute(self.queries['insert_study_keys'], (study, studyUID, accessionNumber))

      return study

  def _get_series(self, study, seriesTags, change):
    seriesUID = seriesTags.get('SeriesInstanceUID', None)
    if seriesUID is None or seriesUID == '':
      self.logger.error('Change %i: Could not find PatientID', change['Seq'])
      return None

    with self.connection.cursor() as c:
      c.execute(self.queries['get_series'], (seriesUID,))
      series = c.fetchone()

      if series is not None:
        series = series[0]
      else:
        modality = seriesTags.get('Modality', None)
        seriesNumber = seriesTags.get('SeriesNumber', None)
        if seriesNumber == '':
          seriesNumber = None
        elif seriesNumber is not None:
          seriesNumber = int(seriesNumber)
        seriesDescription = self._filter_nonASCII(seriesTags.get('SeriesDescription', None))
        bodyPart = self._filter_nonASCII(seriesTags.get('BodyPartExamined', None))

        c.execute(self.queries['insert_series'], (study, None, modality, seriesNumber, seriesDescription, bodyPart))
        c.execute(self.queries['get_identity_series'])
        series = c.fetchone()[0]
        self.logger.info('Added new series to pacsCrawler (DB ID %s)', series)

        c.execute(self.queries['insert_series_keys'], (series, study, seriesUID))
      return series

  def _filter_nonASCII(self, value):
    if value is None:
      return None
    value = str(value)
    res = value
    for c in value:
      if re.match('[^\x20-\x7e]', c) is not None:
        res = res.replace(c, '_')
    return res


def config_logger(**log_config):
  global Logger, UIDlogger

  rootLogger = logging.getLogger('orthanc_forward')

  try:
    log_level = getattr(logging, log_config.get('Level', 'WARNING'))
  except Exception:
    log_level = logging.WARNING

  try:
    verbosity_level = getattr(logging, log_config.get('Verbosity', 'INFO'))
  except Exception:
    verbosity_level = logging.INFO

  formatter = logging.Formatter('[%(asctime)-.19s] %(levelname)-.1s: %(name)s - %(message)s')

  if len(Logger.handlers) == 0:
    print('Adding handler for logger')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler.setLevel(verbosity_level)
    rootLogger.addHandler(handler)

  rootLogger.setLevel(log_level)

  if 'Log_File' in log_config:
    if not os.path.isdir(os.path.dirname(log_config['Log_File'])):
      os.makedirs(os.path.dirname(log_config['Log_File']))
      Logger.info('Creating directory for log files at "%s"', os.path.abspath(os.path.dirname(log_config['Log_File'])))

    handler = logging.handlers.TimedRotatingFileHandler(log_config['Log_File'], when='midnight', backupCount=7)
    handler.setFormatter(formatter)
    handler.setLevel(log_level)
    Logger.addHandler(handler)

  if 'UID_Log' in log_config:
    UIDlogger.setLevel(logging.INFO)

    if not os.path.isdir(os.path.dirname(log_config['UID_Log'])):
      os.makedirs(os.path.dirname(log_config['UID_Log']))
      Logger.info('Creating directory for log files at "%s"', os.path.abspath(os.path.dirname(log_config['UID_Log'])))

    Logger.info('Adding handler for storing Slice SOP Instance UIDs (file: "%s")', log_config['UID_Log'])
    # Add a handler to store UIDs in separate Log file
    # Use a file handler (this will keep growing indefinitely, but only contains the UIDs of each series)
    handler = logging.FileHandler(log_config['UID_Log'], mode='a')
    uid_formatter = logging.Formatter('[%(asctime)-.19s] %(message)s')
    handler.setFormatter(uid_formatter)
    UIDlogger.addHandler(handler)

  Logger.debug('Logging configured')


def main(argv=None):
  global ACCEPTED_MODALITIES, ACCEPTED_SOP_CLASSES, DEBUG_MODE, LAST_CHANGE, CONFIG_FILE, pacscrawlerdb
  config = None
  parser = argparse.ArgumentParser()
  parser.add_argument('config', metavar='CONFIG_FILE', help='JSON configuration file for controlling OrthancForward')
  parser.add_argument('--dry-run', action='store_true', help='Test functionality and validity of configuration without sending the slices')

  args = parser.parse_args(argv)

  if os.path.isfile(args.config):
    CONFIG_FILE = args.config
    with open(args.config) as config_fs:
      config = json.load(config_fs)

  if config is None:
    print('Config not loaded! Exiting')
    exit(1)

  if not isinstance(config, dict):
    print('Expecting configuration to be a dictionary! Exiting')
    exit(1)

  # Configure Logging
  config_logger(**config.get('Logging', {}))

  # Check if loaded settings are valid
  if not _checkSettings(config):
    exit(-1)

  if args.dry_run:
    Logger.info('Running in DEBUG mode (changes are processed, but nothing is sent)')
    DEBUG_MODE = True

  # Check if there is a position holder from earlier runs of this script
  LAST_CHANGE = config.get('Last', 0)

  Logger.debug('Loaded and checked settings: %s', config)

  if "Database_Connection" in config:
    try:
      pacscrawlerdb = PacsCrawlerInterface(**config)
      Logger.info('Initalized PacsCrawler Interface')
    except Exception:
      pacscrawlerdb = None

  try:
    run(**config)
  except KeyboardInterrupt:
    # User quits the listener, store the last processed change (handled by the 'finally' clause) to prevent reprocessing
    Logger.info('Manual interrupt! Closing the script...')
    exit(0)
  except Exception:
    Logger.error('Oh Oh... Something went wrong!', exc_info=True)
    exit(-1)
  finally:  # always try to store the last change position if necessary
    if config.get('Last', 0) < LAST_CHANGE:
      _storeProgress(config)


def run(**config):
  global DEBUG_MODE, LAST_CHANGE, MAX_TRIES, Logger, UIDlogger
  
  interface = OrthancInterface(**config['Connection'])

  # Customization variables
  accepted_modalities = config['Target'].get('Modalities', None)
  accepted_sop_classes = config['Target'].get('SOP_Classes', None)
  target = config['Target']['AET']

  sleep_time = config.get('SleepTime', 1)

  # Check if the target is known in the Orthanc Server
  modalities = interface.getResponse('/modalities')
  if target not in modalities:
    raise Exception('Target %s is not known in the server (available targets: %s)' % (target, modalities))

  http_tries = 0  # Holds the amount of tries for sending the current series being processed. Is reset when slice is successfully sent.
  curSeries = None  #  Holds the current Series being processed (needed for writing out the skipped series when it failed 5 times...)
  curChange = 0

  Logger.info('Welcome to the Orthanc Forward script! Starting the forwarding loop...')

  while True:
    try:
      # Get the next batch of changes
      resp = interface.getResponse('/changes?limit=4&since=%s' % LAST_CHANGE)
      for change in resp.get('Changes', []):
        curChange = change['Seq']

        # Check if the change concerns a new Series
        if change.get('ChangeType', '') == 'NewSeries':
          Logger.debug('Found new series: %s', change['ID'])
          series = interface.getResponse('/series/%s' % change['ID'])

          if 'Modality' not in series['MainDicomTags']:
            Logger.warning('could not find Modality in series %s, skipping...', change['ID'])
            continue

          modality = series['MainDicomTags']['Modality']

          if accepted_modalities is not None and modality not in accepted_modalities:
            Logger.info('New Series (%s) had modality %s, skipping...', change['ID'], modality)
            continue

          instances = series['Instances']

          # Slices found, containing an ID (needed to forward it)
          if len(instances) > 0:
            slice_id = instances[0]  # (string) instance ID of the single slice to send
            # Extract SOP Class UID from the instance, as it is sometimes missing from the series shared-tags
            if accepted_sop_classes is not None:
              tags = interface.getResponse('/instances/%s/simplified-tags' % slice_id)
              if 'SOPClassUID' not in tags:
                Logger.warning('Missing SOPClassUID in tags for series %s, skipping...', change['ID'])
                continue
              if tags['SOPClassUID'] not in accepted_sop_classes:
                Logger.info('New Series (%s) had SOP class %s, skipping...', change['ID'], tags['SOPClassUID'])
                continue

            # Everything checks out, so let's try to send it to our destionation shall we?
            curSeries = change['ID']

            if DEBUG_MODE:
              # Unless you're debugging of course... just write it out to the log
              Logger.info('<DEBUGGING> Change %i: pretending to send slice (ID: %s) from study %s to %s', change['Seq'], slice_id, change['ID'], target)
              UIDlogger.info(series['MainDicomTags']['SeriesInstanceUID'])  # Log UID separately
            else:
              Logger.info('Sending slice (ID: %s) from study %s to %s', slice_id, change['ID'], target)
              UIDlogger.info(series['MainDicomTags']['SeriesInstanceUID'])  # Log UID separately
              interface.postResponse('/modalities/%s/store' % target, [slice_id])  # Send the slice to the target

            if pacscrawlerdb is not None:
              pacscrawlerdb.process_new_series(change, series, interface)

            http_tries = 0  # Slice sent successfully, so reset the counter
        LAST_CHANGE = curChange  # Successfully processed this change, so increase the LAST_CHANGE counter to preven reprocessing

      # This is sort of superfluous, as it is already incremented in the loop, but do it anyway to ensure the next batch is obtained correctly
      LAST_CHANGE = resp.get('Last', LAST_CHANGE) 

      if resp.get('Done', False):
        # Store the current last position in the settings file (in case the program gets killed before progress can be stored)
        if config.get('Last', 0) < LAST_CHANGE:
          Logger.info('Reached last change (for now...)')
          _storeProgress(config)
        Logger.debug('Last change processed, sleeping for %s seconds', sleep_time)
        time.sleep(sleep_time)  # Sleep for n second(s)
    except HTTPError as httpe:
      # Something is not working in the communication to the server
      http_tries += 1

      if http_tries <= MAX_TRIES:
        # Maybe it is restarting? Be patient my young padawan!
        Logger.warning('Received a HTTP error (change No %d, try %d/%d), sleeping for 5 mins...', curChange, http_tries, MAX_TRIES)
        time.sleep(300)  # sleep for 5 minutes... and try again!
      else:
        # Damn, thats going wrong a lot!! skip this series for now, let's continue with stuff that does work (hopefully)
        LAST_CHANGE = curChange  # By setting the LAST_CHANGE to curChange, this change will be skipped on the next try
        http_tries = 0  # Skipping this series, so we can reset our tries.

        Logger.error('Received %d HTTP errors in a row (for series %s, change No %d). Skipping this series (write series id to "skipped_series.txt"',
        MAX_TRIES, curSeries, curChange)
        with open('skipped_series.txt', mode='a') as skipped_file:
          skipped_file.write(curSeries + '\n')
    except URLError:
      Logger.warning('Could not connect to Orthanc server! Retrying in 5 mins...')
      time.sleep(300)


def _checkSettings(settings):
  """
  Checks if required settings are present.
  """
  global Logger
  if 'Connection' not in settings:
    Logger.error('missing Connection settings for the orthanc server (key "Connection")')
    return False
  if 'URL' not in settings['Connection']:
    Logger.error('missing url ("http://<ip>:<port>") of the orthanc server (key "url" in Connection settings)')
    return False

  if 'Target' not in settings:
    Logger.error('missing Target settings for the destination server (key "Target")')
    return False
  if 'AET' not in settings['Target']:
    Logger.error('missing Target (<AET>) of the destination server (key "AET" in Target settings)')
    return False

  return True


def _storeProgress(config):
  global Logger, LAST_CHANGE, CONFIG_FILE
  Logger.info('Storing current position in the settings file')
  config['Last'] = LAST_CHANGE
  with open(CONFIG_FILE, mode='w') as settings_file_fs:
    json.dump(config, settings_file_fs, indent=2)


if __name__ == '__main__':
  main()
