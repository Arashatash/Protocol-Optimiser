# Spec-Driven Development: Radiology Protocol Optimiser (MVP)

## 1. Project Overview
We are building a local, Python-based MVP of an AI agent that monitors MRI DICOM files for two distinct value propositions:
1.  **Sequence-Level QC (Protocol Drift):** Detecting if scan parameters (TE, TR) deviate from a predefined "Gold Standard."
2.  **Academic Surveillance (Missing Sequences):** Detecting if a specific study type (e.g., "MRI Brain MS") is missing clinically required sequences (e.g., SWI).

## 2. Environment & Tech Stack
* **OS:** Windows
* **Language:** Python 3.10+
* **Libraries:** * `pydicom` (to parse DICOM headers)
    * `watchdog` (to monitor the Windows directory for new files)
    * `rich` (for a beautiful, readable CLI output)
    * `json` (standard library, to store our protocol rules)

## 3. Directory Structure
Cursor should help set up the following structure:
/protocol-optimiser-mvp
  ├── main.py                 # Entry point and watchdog observer
  ├── dicom_parser.py         # Logic for reading DICOM tags safely
  ├── rule_engine.py          # Compares parsed data against rules.json
  ├── rules.json              # The "Gold Standard" parameters database
  ├── /dicom_dropzone         # The folder we monitor for new .dcm files
  └── /logs                   # Simple text logs of alerts

## 4. Core Workflows & Logic

### A. The File Watcher (`main.py`)
* Continuously monitor the `/dicom_dropzone` directory.
* When a new `.dcm` file is pasted into the folder, wait 1 second (to ensure the file transfer is complete), then pass the file path to `dicom_parser.py`.

### B. The DICOM Parser (`dicom_parser.py`)
* Read the file using `pydicom.dcmread()`.
* Extract the following tags safely (handle exceptions if tags are missing):
    * Study Description (0008, 1030) -> e.g., "MRI BRAIN W WO CONTRAST"
    * Series Description (0008, 103E) -> e.g., "AX T2 FLAIR"
    * Repetition Time / TR (0018, 0080)
    * Echo Time / TE (0018, 0081)

### C. The Rule Engine (`rule_engine.py` + `rules.json`)
* **Logic 1 (Drift Check):** If the Series Description matches a known series in `rules.json` (e.g., "AX T2 FLAIR"), check if the extracted TE and TR are within an acceptable range (e.g., TE between 90-110). If outside the range, flag as **"PROTOCOL DRIFT DETECTED."**
* **Logic 2 (Missing Sequence Check):** If the Study Description matches a known study type in `rules.json` (e.g., "MRI Brain MS"), keep a running list of all Series Descriptions processed for that study. If the watcher finishes processing a batch of files and "SWI" or "Susceptibility" was not found, flag as **"MISSING CRITICAL SEQUENCE."**

### D. The Output
* Use the `rich` library to print alerts to the terminal.
* Green text: "Scan passes QC."
* Red/Yellow text: Display the specific deviations (e.g., "Expected TE: 100ms. Actual TE: 70ms").

## 5. Execution Plan for AI Assistant
Please build this step-by-step, verifying functionality before moving to the next phase:
* **Phase 1:** Scaffold the project structure and create a basic `rules.json` with dummy data for an MS Brain MRI.
* **Phase 2:** Write `dicom_parser.py` and write a quick test script to ensure we can extract TE, TR, and Descriptions from a sample DICOM.
* **Phase 3:** Write `rule_engine.py` to compare the parsed dictionary against `rules.json`.
* **Phase 4:** Implement `watchdog` in `main.py` to tie it all together into a live, running background process.