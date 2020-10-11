#!/usr/bin/python3
# coding=utf8

import os
import logging
import time
import re
import shutil
import sqlite3
import hashlib
import glob

DB_CONNECTION = None


def get_hash(filename):
    sha256_hash = hashlib.sha256()
    with open(filename, "rb") as file_handle:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: file_handle.read(4096), b""):
            sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()


def open_database(config):
    global DB_CONNECTION

    connection = sqlite3.connect(os.path.join(config, 'documents.db'))

    cursor = connection.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS documents (
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

    cursor = connection.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS document_logs (
            name TEXT,
            timestamp TEXT,
            log TEXT
            )''')
    connection.commit()

    DB_CONNECTION = connection

    return connection


def get_database():
    global DB_CONNECTION
    return DB_CONNECTION


def close_database(connection):
    connection.close()


def add_document(name_original, name, hash_original, status):
    connection = get_database()
    cursor = connection.cursor()

    try:
        cursor.execute(
            'INSERT INTO documents (name_original, hash_original, name, status, last_update) VALUES (?, ?, ?, ?, datetime("now"))',
            (name_original, hash_original, name, status))
    except sqlite3.IntegrityError:
        # Document already present in database
        return False

    connection.commit()
    return True


def add_ocr_hash(name, hash_ocr):
    connection = get_database()
    cursor = connection.cursor()
    cursor.execute(
        'INSERT OR IGNORE INTO documents (name, status, last_update) VALUES (?, ?, datetime("now"))',
        (name, "new"))
    logging.debug("Updating %s with %s", name, hash_ocr)
    cursor.execute(
        'UPDATE documents SET hash_ocr=?, status=?, last_update=datetime("now") WHERE name=?',
        (hash_ocr, "ocred", name))
    connection.commit()


def add_ocr_parameters(filename, values):
    connection = get_database()
    cursor = connection.cursor()
    cursor.execute(
        'UPDATE documents SET ocr_pages=?, ocr_time=?, ocr_errors=?, ocr_warnings=?, ocr_chars_total=?, ocr_chars_wrong=? WHERE name=?',
        (values["Pages"], values["Time"], values["Errors"], values["Warnings"],
         values["Chars_Total"], values["Chars_Wrong"], filename))
    connection.commit()


def update_status(name, status):
    connection = get_database()
    cursor = connection.cursor()
    cursor.execute(
        'UPDATE documents SET status=?, last_update=datetime("now") WHERE name=?',
        (status, name))
    connection.commit()


def save_log(name, log):
    connection = get_database()
    cursor = connection.cursor()
    cursor.execute(
        'INSERT INTO document_logs (name, timestamp, log) VALUES (?, datetime("now"), ?)',
        (name, log))
    connection.commit()


def read_prefix(directory, filename):
    file_handle = open(os.path.join(directory, filename), "r")
    prefix = file_handle.read()
    file_handle.close()
    return prefix.strip()


def get_index(directory):
    files = os.listdir(directory)
    return len(files)


def process_scanner_file(directory, filename, prefix, ocr_in, archive_raw):
    name = None
    index = get_index(archive_raw)

    logging.info("Handling scanned file %s", filename)

    regex_app = r"^[a-z]*[\.\-_]{1}([0-9]{2,4})[\.\-_]{1}([0-9]{1,2})[\.\-_]{1}([0-9]{1,2})[\.\-_]{1}([0-9]{1,2})[\.\-_]{1}([0-9]{1,2})[\.\-_]{1}([0-9]{1,2})\.pdf$"
    matches = re.match(regex_app, filename)
    if matches is not None:
        name_list = [
            prefix, "{:05d}".format(index),
            matches.group(1),
            matches.group(2),
            matches.group(3),
            matches.group(4),
            matches.group(5),
            matches.group(6)
        ]
        name = "-".join(name_list) + ".pdf"

    regex_scanner = r"^([0-9]{4})([0-9]{2})([0-9]{2})_([0-9]{2})([0-9]{2})([0-9]{2})_[0-9a-zA-Z]+_[0-9]+\.pdf$"
    matches = re.match(regex_scanner, filename)
    if matches is not None:
        name_list = [
            prefix, "{:05d}".format(index),
            matches.group(1),
            matches.group(2),
            matches.group(3),
            matches.group(4),
            matches.group(5),
            matches.group(6)
        ]
        name = "-".join(name_list) + ".pdf"

    # Update Database
    hash_value = get_hash(os.path.join(directory, filename))

    if name is None:
        logging.error("Unable to parse %s, stopping processing!", filename)
        if not add_document(filename, name, hash_value, 'name error'):
            logging.error("%s already present, deleting", filename)
            os.unlink(os.path.join(directory, filename))
        return

    if not add_document(filename, name, hash_value, 'new'):
        logging.error("%s already present, deleting", filename)
        os.unlink(os.path.join(directory, filename))
        return

    # Copy to OCR hot folder
    logging.info("Saving to %s", os.path.join(ocr_in, name))
    shutil.copy2(os.path.join(directory, filename), os.path.join(ocr_in, name))
    os.chmod(os.path.join(ocr_in, name), 0o777)

    # Copy to permanent archive
    logging.info("Saving to %s", os.path.join(archive_raw, name))
    shutil.copy2(
        os.path.join(directory, filename), os.path.join(archive_raw, name))
    os.chmod(os.path.join(archive_raw, name), 0o777)

    # Remove input file
    os.unlink(os.path.join(directory, filename))


def process_ocred_file(directory, filename, consumption, archive_ocred):
    logging.info("Handling OCRed file %s", filename)

    logging.info("Saving to %s", os.path.join(consumption, filename))
    shutil.copy2(
        os.path.join(directory, filename), os.path.join(consumption, filename))
    os.chmod(os.path.join(consumption, filename), 0o777)

    logging.info("Saving to %s", os.path.join(archive_ocred, filename))
    shutil.copy2(
        os.path.join(directory, filename), os.path.join(
            archive_ocred, filename))
    os.chmod(os.path.join(archive_ocred, filename), 0o777)

    # Update database
    hash_ocr = get_hash(os.path.join(directory, filename))
    add_ocr_hash(filename, hash_ocr)

    # Read and save OCR parameters
    HFL = glob.glob(os.path.join(directory, "Hot Folder Log*.txt"))
    if len(HFL) > 1:
        logging.error(
            "Found %i Hot Folder Log Files: %s. Deleting all, parsing none.",
            len(HFL), str(HFL))
        for file in HFL:
            os.unlink(file)

    if len(HFL) == 1:
        logging.debug("Parsing %s", HFL[0])
        values = parse_ocr_log(directory, os.path.basename(HFL[0]))
        add_ocr_parameters(filename, values)
        os.unlink(HFL[0])

    # Remove input file
    os.unlink(os.path.join(directory, filename))


def check_status(directory):
    connection = get_database()
    cursor = connection.cursor()

    result = cursor.execute('SELECT name FROM documents WHERE status="ocred"')
    for row in result:
        filename = row[0]

        if not os.path.isfile(os.path.join(directory, filename)):
            logging.info("%s appears to have been consumed", filename)
            update_status(filename, "consumed")
        else:
            logging.info("%s has not yet been consumed", filename)

    connection.commit()


def serve_ocr_queue(directory, filename, ocr_in):
    if len(os.listdir(ocr_in)) > 0:
        return

    logging.info("Starting OCR of %s", filename)
    shutil.move(
        os.path.join(directory, filename), os.path.join(ocr_in, filename))
    os.chmod(os.path.join(ocr_in, filename), 0o777)

    update_status(filename, "ocring")


def parse_ocr_log(directory, filename):
    regex_pages = r"^Verarbeitete Seiten:[ \t]*([0-9]+).$"
    regex_time = r"^Erkennungszeit:[ \t]*([0-9]+) Stunden ([0-9]+) Minuten ([0-9]+) Sekunden.$"
    regex_error = r"^Fehler/Warnungen:[ \t]*([0-9]+) / ([0-9]+).$"
    regex_quali = r"^Nicht eindeutige Zeichen:[ \t]*([0-9]+) % \(([0-9]+) / ([0-9]+)\).$"
    regex_reason = r"^[0-9\., :\t]+Fehler: (.*)$"

    path = os.path.join(directory, filename)

    file = open(path, encoding='utf-16le')
    content = file.readlines()
    file.close()

    result = {
        "Pages": None,
        "Time": None,
        "Errors": None,
        "Warnings": None,
        "Chars_Total": None,
        "Chars_Wrong": None,
        "Error_Message": None,
        "Successful": True,
    }

    for line in content:
        matches = re.match(regex_pages, line)
        if matches is not None:
            result["Pages"] = int(matches.group(1))

        matches = re.match(regex_time, line)
        if matches is not None:
            result["Time"] = int(matches.group(1)) * 3600 + int(
                matches.group(2)) * 60 + int(matches.group(3))

        matches = re.match(regex_error, line)
        if matches is not None:
            result["Errors"] = int(matches.group(1))
            result["Warnings"] = int(matches.group(2))

        matches = re.match(regex_quali, line)
        if matches is not None:
            result["Chars_Total"] = int(matches.group(3))
            result["Chars_Wrong"] = int(matches.group(2))

        matches = re.match(regex_reason, line)
        if matches is not None:
            result["Successful"] = False
            result["Error_Message"] = matches.group(1)

    logging.debug("OCR parameters: %s", str(result))

    return result


def main():
    # Setup logging
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%d.%m.%Y %H:%M:%S',
        level=logging.DEBUG)

    # Directory config
    dirs = {
        "scanner_out": "01_scanner_out",
        "ocr_queue": "02_ocr_queue",
        "ocr_in": "03_ocr_in",
        "ocr_out": "04_ocr_out",
        "ocr_fail": "04_ocr_fail",
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
        except OSError:  # FileExistsError:
            # Don't care
            continue
        logging.info("Created %s", dirs[index])

    # Setup database
    logging.debug("Initializing SQLite DB")
    connection = open_database(dirs["config"])

    last_scanner_out = 0
    last_ocr_out = 0
    last_ocr_queue = 0
    last_consumption = 0
    last_info = 0

    logging.debug("Starting busy loop")
    while True:
        # Read current prefix
        prefix = read_prefix(dirs["config"], "PREFIX")

        if (time.time() - last_info) >= 600:
            logging.info("Prefix: %s", prefix)
            last_info = time.time()

        # Process all files coming in from the scanner
        if (time.time() - last_scanner_out) >= 6:
            logging.debug("Processing %s", dirs["scanner_out"])
            files = os.listdir(dirs["scanner_out"])
            for file in files:
                if not os.path.isfile(os.path.join(dirs["scanner_out"], file)):
                    continue

                filename, file_extension = os.path.splitext(file)
                if file_extension != ".pdf":
                    continue

                process_scanner_file(dirs["scanner_out"], file, prefix,
                                     dirs["ocr_queue"], dirs["archive_raw"])

            last_scanner_out = time.time()

        # Process all files coming out of OCR
        if (time.time() - last_ocr_out) >= 5:
            logging.debug("Processing %s", dirs["ocr_out"])

            files = glob.glob(os.path.join(dirs["ocr_out"], "*.pdf"))
            for fullfile in files:
                file = os.path.basename(fullfile)
                filename, file_extension = os.path.splitext(file)
                process_ocred_file(dirs["ocr_out"], file, dirs["consumption"],
                                   dirs["archive_ocred"])

            files = glob.glob(
                os.path.join(dirs["ocr_out"], "Hot Folder Log*.txt"))
            for fullfile in files:
                file = os.path.basename(fullfile)
                filename, file_extension = os.path.splitext(file)
                logging.error("Found stale file %s in %s. Parsing", file,
                              dirs["ocr_out"])

                stats = parse_ocr_log(dirs["ocr_out"], file)
                os.unlink(os.path.join(dirs["ocr_out"], file))

                if stats["Successful"]:
                    logging.info("OCR was successful, deleted stale log")
                    continue

                # OCR seems to have failed - update status and move away file
                failed_ocr = glob.glob(os.path.join(dirs["ocr_in"], "*.pdf"))
                if len(failed_ocr) == 0:
                    logging.error("Failed OCR: Input vanished, deleting log")
                    os.unlink(os.path.join(dirs["ocr_out"], file))
                elif len(failed_ocr) == 1:
                    filename = os.path.basename(failed_ocr[0])
                    logging.error("OCR for %s failed with %s, moving to %s",
                                  filename, stats["Error_Message"],
                                  dirs["ocr_fail"])
                    shutil.move(failed_ocr[0],
                                os.path.join(dirs["ocr_fail"], filename))
                    os.chmod(os.path.join(dirs["ocr_fail"], filename), 0o777)

                    update_status(filename, "ocr_failed")
                    save_log(filename, stats["Error_Message"])
                elif len(failed_ocr) > 1:
                    logging.error(
                        "Failed OCR: Multiple OCR files in queue, aborting (%s)",
                        str(failed_ocr))
                    time.sleep(86400)
            last_ocr_out = time.time()

        # Serve the OCR queue
        if (time.time() - last_ocr_queue) >= 5:
            logging.debug("Processing %s", dirs["ocr_queue"])
            logging.info("OCR Queue is at %i", len(os.listdir(dirs["ocr_in"])))
            files = os.listdir(dirs["ocr_queue"])
            for file in files:
                if not os.path.isfile(os.path.join(dirs["ocr_queue"], file)):
                    continue

                filename, file_extension = os.path.splitext(file)
                if file_extension != ".pdf":
                    continue

                serve_ocr_queue(dirs["ocr_queue"], file, dirs["ocr_in"])
            last_ocr_queue = time.time()

        # Check for status of all files in the DB
        if (time.time() - last_consumption) >= 600:
            check_status(dirs["consumption"])
            last_consumption = time.time()

        time.sleep(1)

    close_database(connection)


if __name__ == "__main__":
    main()
