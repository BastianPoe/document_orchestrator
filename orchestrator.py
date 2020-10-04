#!/usr/bin/python3
# coding=utf8

import os
import logging
import time
import re
import shutil
import sqlite3
import hashlib

db_connection = None

def getHash(filename):
    sha256_hash = hashlib.sha256()
    with open(filename,"rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096),b""):
            sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

def openDatabase(config):
    global db_connection

    connection = sqlite3.connect(os.path.join(config, 'documents.db'))
    c = connection.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS documents (name TEXT UNIQUE, hash_ocr VARCHAR(64) UNIQUE, name_original TEXT UNIQUE, hash_original VARCHAR(64) UNIQUE, status TEXT, last_update TEXT )''')
    connection.commit()

    db_connection = connection

    return connection

def getDatabase():
    global db_connection
    
    return db_connection

def closeDatabase(connection):
    connection.close()

def addDocument(name_original, name, hash_original, status):
    connection = getDatabase()
    c = connection.cursor()

    try:
        c.execute('INSERT INTO documents (name_original, hash_original, name, status, last_update) VALUES (?, ?, ?, ?, datetime("now"))', (name_original, hash_original, name, status))
    except sqlite3.IntegrityError:
        # Document already present in database
        return False

    connection.commit()
    return True

def addOcrHash(name, hash_ocr):
    connection = getDatabase()
    c = connection.cursor()
    c.execute('INSERT OR IGNORE INTO documents (name, status, last_update) VALUES (?, ?, datetime("now"))', (name, "new"))
    logging.debug("Updating " + name + " with " + hash_ocr);
    c.execute('UPDATE documents SET hash_ocr=?, status=?, last_update=datetime("now") WHERE name=?', (hash_ocr, "ocred", name))
    connection.commit()

def updateStatus(name, status):
    connection = getDatabase()
    c = connection.cursor()
    c.execute('UPDATE documents SET status=?, last_update=datetime("now") WHERE name=?', (status, name))
    connection.commit()


def readPrefix(directory, filename):
    f = open(os.path.join(directory, filename), "r")
    prefix = f.read()
    return prefix.strip()

def getIndex(directory):
    files = os.listdir(directory)
    
    return len(files)

def processScannerFile(directory, filename, prefix, ocr_in, archive_raw):
    name = None
    index = getIndex(dirs["archive_raw"])

    logging.info("Handling scanned file " + filename)

    regex_app = r"^[a-z]*[\.\-_]{1}([0-9]{2,4})[\.\-_]{1}([0-9]{1,2})[\.\-_]{1}([0-9]{1,2})[\.\-_]{1}([0-9]{1,2})[\.\-_]{1}([0-9]{1,2})[\.\-_]{1}([0-9]{1,2})\.pdf$"
    m = re.match(regex_app, filename)
    if m is not None:
        name_list = [prefix, "{:05d}".format(index), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6)]
        name = "-".join(name_list) + ".pdf"

    regex_scanner = r"^([0-9]{4})([0-9]{2})([0-9]{2})_([0-9]{2})([0-9]{2})([0-9]{2})_[0-9a-zA-Z]+_[0-9]+\.pdf$"
    m = re.match(regex_scanner, filename)
    if m is not None:
        name_list = [prefix, "{:05d}".format(index), m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6)]
        name = "-".join(name_list) + ".pdf"
    
    # Update Database
    hash = getHash(os.path.join(directory, filename))

    if name is None:
        logging.error("Unable to parse " + filename + ". Stopping processing!")
        if not addDocument(filename, name, hash, 'name error'):
            logging.error(filename + " already present, deleting")
            os.unlink(os.path.join(directory, filename))
        return

    if not addDocument(filename, name, hash, 'new'):
        logging.error(filename + " already present, deleting")
        os.unlink(os.path.join(directory, filename))
        return

    # Copy to OCR hot folder
    logging.info("Saving to " + os.path.join(ocr_in, name))
    shutil.copyfile(os.path.join(directory, filename), os.path.join(ocr_in, name))
    
    # Copy to permanent archive
    logging.info("Saving to " + os.path.join(archive_raw, name))
    shutil.copyfile(os.path.join(directory, filename), os.path.join(archive_raw, name))
    
    # Remove input file
    os.unlink(os.path.join(directory, filename))

def processOcredFile(directory, filename, consumption, archive_ocred):
    logging.info("Handling OCRed file " + filename)

    logging.info("Saving to " + os.path.join(consumption, filename))
    shutil.copyfile(os.path.join(directory, filename), os.path.join(consumption, filename))

    logging.info("Saving to " + os.path.join(archive_ocred, filename))
    shutil.copyfile(os.path.join(directory, filename), os.path.join(archive_ocred, filename))

    # Update database
    hash_ocr = getHash(os.path.join(directory, filename))
    addOcrHash(filename, hash_ocr)

    # Remove input file
    os.unlink(os.path.join(directory, filename))

def checkStatus(directory):
    connection = getDatabase()
    c = connection.cursor()
    
    result = c.execute('SELECT name FROM documents WHERE status="ocred"')
    for row in result:
        filename = row[0]
        
        if not os.path.isfile(os.path.join(directory, filename)):
            logging.info(filename + " appears to have been consumed")
            updateStatus(filename, "consumed")
        else:
            logging.info(filename + " has not yet been consumed")
    
    connection.commit()


# Setup logging
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', datefmt='%d.%m.%Y %H:%M:%S', level=logging.DEBUG)

# Directory config
dirs = {
    "scanner_out": "01_scanner_out",
    "ocr_in": "02_ocr_in",
    "ocr_out": "03_ocr_out",
    "consumption": "04_paperless_in",
    "storage": "05_paperless_storage",
    "archive_ocred": "archive_ocr",
    "archive_raw": "archive_raw",
    "config": "config",
    "logs": "logs"
}

logging.info("Creating working directories")
for index in dirs:
    try:
        os.mkdir(dirs[index])
    except OSError: # FileExistsError:
        # Don't care
        continue
    logging.info("Created %s" % dirs[index])

# Setup database
logging.debug("Initializing SQLite DB")
connection = openDatabase(dirs["config"])

logging.debug("Starting busy loop")
while True:
    logging.debug("Looping")

    # Read current prefix
    prefix = readPrefix(dirs["config"], "PREFIX")
    
    # Process all files coming in from the scanner
    files = os.listdir(dirs["scanner_out"])
    for file in files:
        if not os.path.isfile(os.path.join(dirs["scanner_out"], file)):
            continue
        processScannerFile(dirs["scanner_out"], file, prefix, dirs["ocr_in"], dirs["archive_raw"])

    # Process all files coming out of OCR
    files = os.listdir(dirs["ocr_out"])
    for file in files:
        if not os.path.isfile(os.path.join(dirs["ocr_out"], file)):
            continue
        processOcredFile(dirs["ocr_out"], file, dirs["consumption"], dirs["archive_ocred"])

    # Check for status of all files in the DB
    checkStatus(dirs["consumption"])

    time.sleep(60)

closeDatabase(connection)
