# gamma-bits
Python script to extract data from ORTEC MAESTRO List Mode files (.Lis). Specifically tailored to operation of the ORTEC IDM-200-V, which shares file formats with ORTEC DSpec Pro and DSpec 50/502 instruments.

### Usage from command line:
```
python process_listmode.py inputFile.Lis outFileName.csv
```
### Contents: 
- ```process_listmode.py```: Python script to be run on the list mode (binary) file.
- ```sample_data/```: folder with sample input list mode file and corresponding output .csv file.

### Data structure explanation:
The .Lis file encoding provided by MAESTRO is outlined in the hardware manual that accompanies each device, as well as here: 
https://www.ortec-online.com/-/media/ametekortec/manuals/l/list-mode-file-formats.pdf?la=en&revision=0b78b32a-9ee7-4243-b4ac-a8adb886381d

The file is a byte-stream of data that is parsed into a 256-byte header and subsequent data. This script is written for the ORTEC IDM-200-V (and other PRO List formats), which has data consisting of a string of 32-bit words. Words are encoded by type. The currently supported data that are parsed to the output .csv file are
- ADC value
- Time stamp (combining the coarse and fine time stamps provided by the device)
