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
    c.execute('''CREATE TABLE IF NOT EXISTS documents (
            name TEXT UNIQUE,
            hash_ocr VARCHAR(64) UNIQUE,
            name_original TEXT UNIQUE,
            hash_original VARCHAR(64) UNIQUE,
            status TEXT,
            last_update TEXT,
            ocr_pages INTEGER,
            ocr_time INTEGER,
            ocr_errors INTEGER,
            ocr_warnings INTEGER,
            ocr_chars_total INTEGER,
            ocr_chars_wrong INTEGER
            )''')
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

def addOcrParameters(filename, values):
    connection = getDatabase()
    c = connection.cursor()
    c.execute('UPDATE documents SET ocr_pages=?, ocr_time=?, ocr_errors=?, ocr_warnings=?, ocr_chars_total=?, ocr_chars_wrong=? WHERE name=?', (values["Pages"], values["Time"], values["Errors"], values["Warnings"], values["Chars_Total"], values["Chars_Wrong"], filename))
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

    # Read and save OCR parameters
    if os.path.isfile(os.path.join(directory, "Hot Folder Log.txt")):
        values = ParseOcrLog(directory, "Hot Folder Log.txt")
        addOcrParameters(filename, values)
        os.unlink(os.path.join(directory, "Hot Folder Log.txt"))

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

def serveOcrQueue(directory, filename, ocr_in):
    if len(os.listdir(ocr_in)) > 0:
        return

    shutil.move(os.path.join(directory, filename), os.path.join(ocr_in, filename))

    updateStatus(filename, "ocring")

def ParseOcrLog(directory, filename):
    regex_pages = r"^Verarbeitete Seiten:[ \t]*([0-9]+).$"
    regex_time  = r"^Erkennungszeit:[ \t]*([0-9]+) Stunden ([0-9]+) Minuten ([0-9]+) Sekunden.$"
    regex_error = r"^Fehler/Warnungen:[ \t]*([0-9]+) / ([0-9]+).$"
    regex_quali = r"^Nicht eindeutige Zeichen:[ \t]*([0-9]+) % \(([0-9]+) / ([0-9]+)\).$"

    path = os.path.join(directory, filename);

    file = open(path, encoding='utf-16le');
    content = file.readlines();
    file.close();

    result = {
            "Pages": None,
            "Time": None,
            "Errors": None,
            "Warnings": None,
            "Chars_Total": None,
            "Chars_Wrong": None
            };

    for line in content:
        m = re.match(regex_pages, line);
        if m is not None:
            result["Pages"] = int(m.group(1));

        m = re.match(regex_time, line);
        if m is not None:
            result["Time"] = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3));

        m = re.match(regex_error, line);
        if m is not None:
            result["Errors"] = int(m.group(1));
            result["Warnings"] = int(m.group(2));

        m = re.match(regex_quali, line);
        if m is not None:
            result["Chars_Total"] = int(m.group(3));
            result["Chars_Wrong"] = int(m.group(2));

    return result;

# Setup logging
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', datefmt='%d.%m.%Y %H:%M:%S', level=logging.DEBUG)

# Directory config
dirs = {
    "scanner_out": "01_scanner_out",
    "ocr_queue": "02_ocr_queue",
    "ocr_in": "03_ocr_in",
    "ocr_out": "04_ocr_out",
    "consumption": "05_paperless_in",
    "storage": "06_paperless_storage",
    "archive_ocred": "archive_ocr",
    "archive_raw": "archive_raw",
    "config": "config"
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

last_scanner_out = 0
last_ocr_out = 0
last_ocr_queue = 0
last_consumption = 0

logging.debug("Starting busy loop")
while True:
    # Read current prefix
    prefix = readPrefix(dirs["config"], "PREFIX")

    # Process all files coming in from the scanner
    if (time.time() - last_scanner_out) >= 60:
        logging.debug("Processing " + dirs["scanner_out"])
        files = os.listdir(dirs["scanner_out"])
        for file in files:
            if not os.path.isfile(os.path.join(dirs["scanner_out"], file)):
                continue

            filename, file_extension = os.path.splitext(file)
            if file_extension != ".pdf":
                continue

            processScannerFile(dirs["scanner_out"], file, prefix, dirs["ocr_queue"], dirs["archive_raw"])

        last_scanner_out = time.time()

    # Process all files coming out of OCR
    if (time.time() - last_ocr_out) >= 5:
        logging.debug("Processing " + dirs["ocr_out"])
        files = os.listdir(dirs["ocr_out"])
        for file in files:
            if not os.path.isfile(os.path.join(dirs["ocr_out"], file)):
                continue

            filename, file_extension = os.path.splitext(file)
            if file_extension != ".pdf":
                continue

            processOcredFile(dirs["ocr_out"], file, dirs["consumption"], dirs["archive_ocred"])
        last_ocr_out = time.time()

    # Serve the OCR queue
    if (time.time() - last_ocr_queue) >= 5:
        logging.debug("Processing " + dirs["ocr_queue"])
        logging.info("OCR Queue is at " + str(len(os.listdir(dirs["ocr_in"]))))
        files = os.listdir(dirs["ocr_queue"])
        for file in files:
            if not os.path.isfile(os.path.join(dirs["ocr_queue"], file)):
                continue

            filename, file_extension = os.path.splitext(file)
            if file_extension != ".pdf":
                continue

            serveOcrQueue(dirs["ocr_queue"], file, dirs["ocr_in"])
        last_ocr_queue = time.time()

    # Check for status of all files in the DB
    if (time.time() - last_consumption) >= 600:
        checkStatus(dirs["consumption"])
        last_consumption = time.time()

    time.sleep(1)

closeDatabase(connection)
