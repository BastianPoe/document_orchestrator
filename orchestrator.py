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
from datetime import datetime
import subprocess

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
        cursor.execute('''INSERT INTO documents
                (name_original, hash_original, name, status, last_update)
                VALUES (?, ?, ?, ?, datetime("now"))''',
                       (name_original, hash_original, name, status))
    except sqlite3.IntegrityError:
        # Document already present in database
        return False

    connection.commit()
    return True


def add_ocr_hash(name, hash_ocr):
    connection = get_database()
    cursor = connection.cursor()
    cursor.execute('''INSERT OR IGNORE INTO documents
        (name, status, last_update)
        VALUES (?, ?, datetime("now"))''', (name, "new"))
    logging.debug("Updating %s with %s", name, hash_ocr)
    cursor.execute('''UPDATE documents SET
        hash_ocr=?, status=?, last_update=datetime("now") WHERE name=?''',
                   (hash_ocr, "ocred", name))
    connection.commit()


def add_ocr_parameters(filename, values):
    connection = get_database()
    cursor = connection.cursor()
    cursor.execute(
        '''UPDATE documents SET
        ocr_pages=?, ocr_time=?, ocr_errors=?, ocr_warnings=?,
        ocr_chars_total=?, ocr_chars_wrong=? WHERE name=?''',
        (values["Pages"], values["Time"], values["Errors"], values["Warnings"],
         values["Chars_Total"], values["Chars_Wrong"], filename))
    connection.commit()


def update_status(name, status):
    connection = get_database()
    cursor = connection.cursor()
    cursor.execute('''UPDATE documents SET
        status=?, last_update=datetime("now") WHERE name=?''', (status, name))
    connection.commit()


def update_status_by_original_hash(hash_original, status):
    connection = get_database()
    cursor = connection.cursor()
    cursor.execute('''UPDATE documents SET
        status=?, last_update=datetime("now") WHERE hash_original=?''',
                   (status, hash_original))
    connection.commit()


def save_log(name, log):
    connection = get_database()
    cursor = connection.cursor()
    cursor.execute('''INSERT INTO document_logs
        (name, timestamp, log) VALUES (?, datetime("now"), ?)''', (name, log))
    connection.commit()


def read_prefix(directory, filename):
    file_handle = open(os.path.join(directory, filename), "r")
    prefix = file_handle.read()
    file_handle.close()
    return prefix.strip()


def get_index(directory):
    files = os.listdir(directory)
    return len(files)


def is_file_stable(pathname):
    if (time.time() - os.path.getmtime(pathname)) < 120:
        return False

    return True


def wait_for_file_to_stabilize(pathname):
    if os.path.getmtime(pathname) > time.time():
        logging.error("Time mismatch, please sync your clocks")
        return

    while not is_file_stable(pathname):
        logging.info("Waiting for file %s to stabilize. (%i vs. %i)", pathname,
                     time.time(), os.path.getmtime(pathname))
        time.sleep(120)


def process_scanner_file(directory,
                         filename,
                         prefix,
                         ocr_in,
                         archive_raw,
                         fail,
                         strict=True,
                         suffix=None):
    name = None
    index = get_index(archive_raw)

    logging.info("Handling scanned file %s", filename)
    wait_for_file_to_stabilize(os.path.join(directory, filename))

    regex_app = r"^[a-z]*[\.\-_]{1}([0-9]{2,4})[\.\-_]{1}([0-9]{1,2})" + \
            r"[\.\-_]{1}([0-9]{1,2})[\.\-_]{1}([0-9]{1,2})[\.\-_]{1}" + \
            r"([0-9]{1,2})[\.\-_]{1}([0-9]{1,2})\.pdf$"
    matches = re.match(regex_app, filename)
    if matches is not None:
        name_list = [
            str(prefix), "{:05d}".format(index),
            matches.group(1),
            matches.group(2),
            matches.group(3),
            matches.group(4),
            matches.group(5),
            matches.group(6),
            str(suffix)
        ]
        name = "-".join(name_list) + ".pdf"

    regex_scanner = r"^([0-9]{4})([0-9]{2})([0-9]{2})_([0-9]{2})([0-9]{2})" + \
            r"([0-9]{2})_[0-9a-zA-Z]+_[0-9]+\.pdf$"
    matches = re.match(regex_scanner, filename)
    if matches is not None:
        name_list = [
            str(prefix), "{:05d}".format(index),
            matches.group(1),
            matches.group(2),
            matches.group(3),
            matches.group(4),
            matches.group(5),
            matches.group(6),
            str(suffix)
        ]
        name = "-".join(name_list) + ".pdf"

    regex_heuristic = r"([0-9]{4})[\-\._]{1}([0-9]{2})[\-\._]{1}([0-9]{2})" + \
            r"[\-\._]{1}([0-9]{2})[\-\._]{1}([0-9]{2})[\-\._]{1}([0-9]{2})" + \
            r".*\.pdf$"
    matches = re.match(regex_heuristic, filename)
    if name is None and matches is not None:
        name_list = [
            str(prefix), "{:05d}".format(index),
            matches.group(1),
            matches.group(2),
            matches.group(3),
            matches.group(4),
            matches.group(5),
            matches.group(6),
            str(suffix)
        ]
        name = "-".join(name_list) + ".pdf"

    if name is None:
        if strict:
            logging.error("Unable to parse %s, moving to %s!", filename, fail)

            shutil.move(
                os.path.join(directory, filename), os.path.join(
                    fail, filename))
            os.chmod(os.path.join(fail, filename), 0o777)
            return

        # Just make up a name as we go along
        filename_no_ext, file_extension = os.path.splitext(filename)

        now = datetime.now()
        name_list = [
            str(prefix), "{:05d}".format(index),
            now.strftime("%Y"),
            now.strftime("%m"),
            now.strftime("%d"),
            now.strftime("%H"),
            now.strftime("%M"),
            now.strftime("%S"),
            str(suffix), filename_no_ext
        ]
        name = "-".join(name_list) + ".pdf"

        logging.info("Created input file filename %s", name)

    # Update Database
    hash_value = get_hash(os.path.join(directory, filename))

    if not add_document(filename, name, hash_value, "new"):
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


def preserve_hfl(filename, hfl):
    logging.debug("preserve_hfl(%s, %s)", filename, hfl)

    real_hfl = os.path.basename(hfl)
    filename, file_extension = os.path.splitext(filename)

    preserve_name = filename + "_" + real_hfl
    logging.debug("preserve_name = %s", preserve_name)

    shutil.move(hfl, os.path.join("logs", preserve_name))
    os.chmod(os.path.join("logs", preserve_name), 0o777)

    logging.debug("preserve done")


def process_ocred_file(directory, filename, consumption, archive_ocred):
    logging.info("Handling OCRed file %s", filename)

    # Make sure file is really done
    wait_for_file_to_stabilize(os.path.join(directory, filename))

    logging.info("Waiting for OCR Log to appear...")
    path = os.path.join(directory, "Hot Folder Log*.txt")
    while len(glob.glob(path)) < 1:
        logging.debug("OCR Logs: %s", str(glob.glob(path)))
        time.sleep(10)

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
    hot_folder_log = glob.glob(os.path.join(directory, "Hot Folder Log*.txt"))
    if len(hot_folder_log) > 1:
        logging.error(
            "Found %i Hot Folder Log Files: %s. Deleting all, parsing none.",
            len(hot_folder_log), str(hot_folder_log))
        for file in hot_folder_log:
            preserve_hfl(filename, file)

    if len(hot_folder_log) == 1:
        logging.debug("Parsing %s", hot_folder_log[0])
        # Make sure file is really done
        wait_for_file_to_stabilize(hot_folder_log[0])

        values = parse_ocr_log(directory, os.path.basename(hot_folder_log[0]))
        add_ocr_parameters(filename, values)
        preserve_hfl(filename, hot_folder_log[0])

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

    connection.commit()


def serve_ocr_queue(directory, filename, ocr_in):
    if len(os.listdir(ocr_in)) > 0:
        return False

    logging.info("Starting OCR of %s", filename)
    shutil.move(
        os.path.join(directory, filename), os.path.join(ocr_in, filename))
    os.chmod(os.path.join(ocr_in, filename), 0o777)

    update_status(filename, "ocring")

    return True


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


def repair_pdf(pathname, ocr_queue):
    dirname = os.path.dirname(pathname)
    filename = os.path.basename(pathname)

    if "_r.pdf" in filename:
        logging.error("%s has been repaired and failed again", filename)
        return

    new_filename = filename.replace(".pdf", "_r.pdf")

    logging.info("Trying to repair PDF %s with mutool", filename)
    cmd = "mutool clean '" + pathname + "' '" + os.path.join(
        ocr_queue, new_filename) + "'"
    os.system(cmd)


def cleanup_ocr_in(ocr_in, ocr_fail, ocr_queue, error=None):
    # OCR seems to have failed - update status and move away file
    failed_ocr = glob.glob(os.path.join(ocr_in, "*.pdf"))
    if len(failed_ocr) == 0:
        logging.error("Failed OCR: Input vanished, deleting log")
        return True

    if len(failed_ocr) == 1:
        # Make sure file is really done
        wait_for_file_to_stabilize(failed_ocr[0])

        # Try to repair the PDF and put it into the queue again
        repair_pdf(failed_ocr[0], ocr_queue)

        # Put pdf into failed folder
        filename = os.path.basename(failed_ocr[0])
        logging.error("OCR for %s failed with %s, moving to %s", filename,
                      error, ocr_fail)
        shutil.move(failed_ocr[0], os.path.join(ocr_fail, filename))
        os.chmod(os.path.join(ocr_fail, filename), 0o777)

        update_status(filename, "ocr_failed")
        save_log(filename, error)
        return True

    if len(failed_ocr) > 1:
        logging.error("Failed OCR: Multiple OCR files in queue, aborting (%s)",
                      str(failed_ocr))
        time.sleep(86400)

    return False


def main():
    # Directory config
    dirs = {
        "scanner_in": "01_scanner",
        "mobile_in": "01_mobile",
        "email_in": "01_email",
        "parse_fail": "01_fail",
        "ocr_queue": "02_ocr_queue",
        "ocr_in": "03_ocr_in",
        "ocr_out": "04_ocr_out",
        "ocr_fail": "04_ocr_fail",
        "consumption": "05_consumption",
        "archive_ocred": "archive_ocr",
        "archive_raw": "archive_raw",
        "config": "config",
        "logs": "logs"
    }

    for index in dirs:
        try:
            os.mkdir(dirs[index])
        except OSError:  # FileExistsError:
            # Don't care
            continue

    # Configure logging
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%d.%m.%Y %H:%M:%S',
        level=logging.DEBUG,
        handlers=[
            logging.FileHandler(
                os.path.join(dirs["logs"], "orchestrator.log")),
            logging.StreamHandler()
        ])

    # Setup database
    logging.debug("Initializing SQLite DB")
    connection = open_database(dirs["config"])

    last_scanner_out = 0
    last_ocr_out = 0
    last_ocr_queue = 0
    last_consumption = 0
    last_info = 0
    last_email = 0
    last_ocr_in = time.time()

    logging.debug("Starting busy loop")
    while True:
        # Read current prefix
        prefix = read_prefix(dirs["config"], "PREFIX")

        if (time.time() - last_info) >= 600:
            logging.info("Prefix: %s", prefix)
            last_info = time.time()

        # Process all files coming in from the scanner
        if (time.time() - last_scanner_out) >= 6:
            # logging.debug("Processing %s", dirs["scanner_in"])
            files = glob.glob(os.path.join(dirs["scanner_in"], "*.pdf"))
            for fullfile in files:
                filename = os.path.basename(fullfile)

                # Make sure that files have not been recently changed before touching them
                if not is_file_stable(fullfile):
                    continue

                process_scanner_file(dirs["scanner_in"], filename, prefix,
                                     dirs["ocr_queue"], dirs["archive_raw"],
                                     dirs["parse_fail"], True, "scanner")

            files = glob.glob(os.path.join(dirs["mobile_in"], "*.pdf"))
            for fullfile in files:
                filename = os.path.basename(fullfile)

                # Make sure that files have not been recently changed before touching them
                if not is_file_stable(fullfile):
                    continue

                process_scanner_file(dirs["mobile_in"], filename, None,
                                     dirs["ocr_queue"], dirs["archive_raw"],
                                     dirs["parse_fail"], False, "mobile")

            files = glob.glob(os.path.join(dirs["email_in"], "*.pdf"))
            for fullfile in files:
                filename = os.path.basename(fullfile)

                # Make sure that files have not been recently changed before touching them
                if not is_file_stable(fullfile):
                    continue

                process_scanner_file(dirs["email_in"], filename, None,
                                     dirs["ocr_queue"], dirs["archive_raw"],
                                     dirs["parse_fail"], False, "email")

            last_scanner_out = time.time()

        # Process all files coming out of OCR
        if (time.time() - last_ocr_out) >= 5:
            # logging.debug("Processing %s", dirs["ocr_out"])

            files = glob.glob(os.path.join(dirs["ocr_out"], "*.pdf"))
            for fullfile in files:
                # Make sure that files have not been recently changed before touching them
                wait_for_file_to_stabilize(fullfile)

                file = os.path.basename(fullfile)
                filename, file_extension = os.path.splitext(file)
                process_ocred_file(dirs["ocr_out"], file, dirs["consumption"],
                                   dirs["archive_ocred"])
                last_ocr_in = None

            files = glob.glob(
                os.path.join(dirs["ocr_out"], "Hot Folder Log*.txt"))
            for fullfile in files:
                # Make sure that files have not been recently changed before touching them
                wait_for_file_to_stabilize(fullfile)

                if len(glob.glob(os.path.join(dirs["ocr_out"], "*.pdf"))) > 0:
                    logging.warning(
                        "OCR output PDF suddenly appeared, skipping")
                    break

                file = os.path.basename(fullfile)
                filename, file_extension = os.path.splitext(file)
                logging.error("Found stale file %s in %s. Parsing", file,
                              dirs["ocr_out"])

                stats = parse_ocr_log(dirs["ocr_out"], file)
                preserve_hfl("stale_" + str(time.time()),
                             os.path.join(dirs["ocr_out"], file))

                last_ocr_in = None

                if stats["Successful"]:
                    logging.info("OCR was successful, deleted stale log")
                    continue

                cleanup_ocr_in(dirs["ocr_in"], dirs["ocr_fail"],
                               dirs["ocr_queue"], stats["Error_Message"])
            last_ocr_out = time.time()

        # Serve the OCR queue
        if (time.time() - last_ocr_queue) >= 30:
            # logging.debug("Processing %s", dirs["ocr_queue"])
            if last_ocr_in is not None:
                duration = time.time() - last_ocr_in
            else:
                duration = -1
            logging.info("OCR Queue is at %i since %i s",
                         len(os.listdir(dirs["ocr_in"])), duration)

            if len(glob.glob(os.path.join(dirs["ocr_out"], "*"))) > 0 or len(
                    glob.glob(os.path.join(dirs["ocr_in"], "*"))) > 0:
                logging.warning("Need to process OCR queue first, skipping")
            else:
                files = os.listdir(dirs["ocr_queue"])
                for file in files:
                    if not os.path.isfile(
                            os.path.join(dirs["ocr_queue"], file)):
                        continue

                    filename, file_extension = os.path.splitext(file)
                    if file_extension != ".pdf":
                        continue

                    ret = serve_ocr_queue(dirs["ocr_queue"], file,
                                          dirs["ocr_in"])

                    if ret:
                        last_ocr_in = time.time()
            last_ocr_queue = time.time()

        # Check for status of all files in the DB
        if (time.time() - last_consumption) >= 600:
            check_status(dirs["consumption"])
            last_consumption = time.time()

        time.sleep(1)

        # Check for OCR timeout
        if (last_ocr_in is not None) and len(os.listdir(
                dirs["ocr_in"])) > 0 and (time.time() - last_ocr_in) >= 3600:
            logging.error("OCR timed out after %i, moving to fails",
                          (time.time() - last_ocr_in))

            # Remove files from ocr_in
            cleanup_ocr_in(dirs["ocr_in"], dirs["ocr_fail"], dirs["ocr_queue"],
                           "ocr timeout")

            # Make sure that queue is considered empty
            last_ocr_in = None

            # Make sure that the OCR queue is served right away to avoid delays
            last_ocr_queue = 0

        if last_email is not None and (time.time() - last_email) >= 600:
            email_server = os.environ.get("EMAIL_SERVER")
            email_user = os.environ.get("EMAIL_USER")
            email_pass = os.environ.get("EMAIL_PASS")
            email_folder = os.environ.get("EMAIL_FOLDER")

            if email_folder is None:
                email_folder = "INBOX"

            if email_server is None or email_user is None or email_pass is None:
                logging.info(
                    "Fetching emails is not configured, please set " + \
                            "EMAIL_SERVER, EMAIL_USER and EMAIL_PASS"
                )
                last_email = None
            else:
                subprocess.call([
                    "attachment-downloader", "--host", email_server,
                    "--username", email_user, "--password", email_pass,
                    "--output", dirs["email_in"], "--imap-folder",
                    email_folder, "--delete"
                ])
                last_email = time.time()

    close_database(connection)


if __name__ == "__main__":
    main()
