from dlextract.ArchiveEngine import get_extractor
from pathlib import Path

if __name__ == "__main__":
    # Example URLs for testing different archive formats
    # Uncomment the desired test URL
    # test_url = "http://127.0.0.1:8000/file.zip"
    # test_url = "http://127.0.0.1:8000/file.7z"
    # test_url = "http://127.0.0.1:8000/file.rar"
    # test_url = "https://www.learningcontainer.com/download/sample-large-zip-file/?wpdmdl=1639&refresh=69416ec3d62801765895875"
    # test_url = "https://getsamplefiles.com/download/7z/sample-3.7z"
    # test_url = "" # Add a valid RAR from maybe a trustable "source"

    password = None  # Set password if the archive is encrypted

    # Attempt to get the appropriate extractor for the archive
    extractor = get_extractor(test_url, password)

    if extractor:
        files = extractor.get_files()  # Retrieve the list of files in the archive

        print("Archive map:")
        for f in files:
            print("- " + str(f))

        if files:
            for file in files:
                print(f"Extracting {file}...")
                # Extract each file to the "extracted" directory
                extractor.extract_to_disk(file, Path(f"extracted/{file}"))
            print("Extraction complete.")
