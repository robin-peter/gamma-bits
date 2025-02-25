import sys
import datetime
from tqdm import tqdm, trange
from bitstring import BitArray
from pandas import DataFrame
import numpy as np
from os.path import getsize
import argparse

FINE_TIME_SAMPLE = 200e-9  # 200 ns per fine time sample increment
COARSE_TIME_SAMPLE = 10e-3  # 10 ms per coarse time sample increment
WORD_BIT_LENGTH = 32
HEADER_BYTE_LENGTH = 256
WIN_TIME_ZERO = datetime.datetime(
    1601, 1, 1, 0, 0, 0, 0)  # January 1, 1601 (UTC)


globalCoarseTime = 0  # global real time w.r.t. beginning of acquisition

# To collect data
growingADCArray = []  # in ADC CHA
growingTimeStampArray = []  # in seconds

# To determine the acquisition start time and hardware delay
growingC1Array = []
growingC2Array = []
growingC3Array = []
growingHDWArray = []

# To parse input arguments, including optional chunk size


def parseArgs():
    parser = argparse.ArgumentParser(
        prog="process_listmode_chunk",
        description="process a .Lis file in chunks to avert memory handling problems")

    # Positional arguments
    parser.add_argument('inListModeFile', help="Input binary .Lis file")
    parser.add_argument(
        'outDataFile', help="Name for output data file, with file ext, e.g. data.csv")

    # Optional arg: chunk size with default value of 2 MB
    parser.add_argument('-c', '--chunk_size', type=int, default=2,
                        help="Chunk size (in MB) to read at a time. Default is 2 MB")

    return parser.parse_args()


def handleWord(word):
    # len(32) string of 0s and 1s
    encoderByte = word[-8:]
    if encoderByte[:2] == '11':  # ADC word
        handleDataWord(word)
    elif encoderByte[:2] == '10':  # RT word
        handleRTWord(word)
    elif encoderByte[:2] == '01':  # LT word
        pass  # can be adapted to handle these in the future
    elif encoderByte == '00000000':  # HDW
        handleHDWWord(word)
    elif encoderByte == '00000001':  # C1
        handleC1Word(word)
    elif encoderByte == '00000010':  # C2
        handleC2Word(word)
    elif encoderByte == '00000011':  # C3
        handleC3Word(word)
    else:
        pass


def handleDataWord(dataWord):
    # ADC (energy) value
    readableADCWord = dataWord[-6:] + dataWord[16:24]
    growingADCArray.append(int(readableADCWord, 2))

    # combine fine timestamp with most recent coarse timestamp
    readableFineTimeWord = dataWord[8:16] + dataWord[:8]
    fineTime = FINE_TIME_SAMPLE * int(readableFineTimeWord, 2)
    growingTimeStampArray.append(fineTime + globalCoarseTime)


def handleRTWord(RTWord):
    readableRTWord = RTWord[-6:] + RTWord[16:24] + RTWord[8:16] + RTWord[:8]
    global globalCoarseTime
    globalCoarseTime = COARSE_TIME_SAMPLE * int(readableRTWord, 2)


def handleHDWWord(HDWWord):
    # hardware timestamp that is read out at the end of every call for data from spectrometer
    readableHDWWord = HDWWord[8:16] + HDWWord[:8]
    HDWTime = FINE_TIME_SAMPLE * int(readableHDWWord, 2)
    growingHDWArray.append(HDWTime + globalCoarseTime)


def handleC1Word(C1Word):
    # 3-part windows timestamp that is generated at the start of every call for data from spectrometer
    readableC1Word = C1Word[:-8]
    growingC1Array.append(readableC1Word)


def handleC2Word(C2Word):
    readableC2Word = C2Word[:-8]
    growingC2Array.append(readableC2Word)


def handleC3Word(C3Word):
    readableC3Word = C3Word[:-16]
    growingC3Array.append(readableC3Word)


def processHeader(fileContent):
    pass  # accomodate header later


def loadBitWords(fileContent):
    # Get 32-bit words from bitstring
    ListModeData = fileContent[HEADER_BYTE_LENGTH:]
    ListModeBitString = BitArray(bytes=ListModeData).bin
    ListModeWordArray = [ListModeBitString[i:i+WORD_BIT_LENGTH]
                         for i in trange(0, len(ListModeBitString), WORD_BIT_LENGTH,
                                         desc='Loading words from bitstring')]
    return ListModeWordArray


def win2dt(s, us):
    # convert a time delta from Windows / CONNECTIONS to Python Datetime
    return WIN_TIME_ZERO + datetime.timedelta(seconds=s, microseconds=us)


def convertSamplesToDatetime(samples, C_TIME_SAMPLE=100e-9):
    # convert number of samples from combined CONNECTIONS (C1, C2, C3) filetime to Python Datetime
    # rounding from total seconds to the datetime structure to fix binary representation floating point err
    total_seconds = samples * C_TIME_SAMPLE
    s = total_seconds // 1
    us = round(total_seconds % 1, 3) * 1e6
    time = win2dt(s, us)
    return time


def parseCONNECTIONS(c1, c2, c3):
    # from the individually read 32-bit words, combine C1, C2, C3 into full CONNECTIONS filetime
    # then get UTC datetime as output
    bytes = []
    for i in range(3):
        byte = c1[8*i:8*(i+1)]
        bytes.append(byte)

    for i in range(3):
        byte = c2[8*i:8*(i+1)]
        bytes.append(byte)

    for i in range(2):
        byte = c3[8*i:8*(i+1)]
        bytes.append(byte)

    bytelist = bytes[::-1]
    bitstring = "".join(bytelist)

    result = convertSamplesToDatetime(int(bitstring, 2))
    return result


if __name__ == '__main__':

    args = parseArgs()
    inListModeFile = args.inListModeFile
    outDataFile = args.outDataFile

    # Estimate number of chunks needed based on chunk and file size
    # For progress bar purpose only
    CHUNK_SIZE = args.chunk_size * 1024 * 1024  # Convert MB to B
    BYTES_TO_READ = getsize(inListModeFile) - HEADER_BYTE_LENGTH
    CHUNKS_TO_READ = int(np.ceil(BYTES_TO_READ / CHUNK_SIZE))
    ChunkIdx = 0  # will be used for command line printout

    # Estimated progress bar based on chunk number
    pbar = tqdm(total=CHUNKS_TO_READ,
                desc='Processing chunks ({} MB each)'.format(args.chunk_size))
    header = True  # for saving data, write the .csv header during the first loop only

    with open(inListModeFile, mode='rb') as f:

        # Skip header data
        f.seek(HEADER_BYTE_LENGTH)

        # Process file in chunks
        while True:
            # Save and dump data in every chunk
            growingADCArray = []  # in ADC CHA
            growingTimeStampArray = []  # in seconds

            # Read chunk
            chunk = f.read(CHUNK_SIZE)

            # Break if the end of the file is reached
            if not chunk:
                break

            # For each chunk, Get 32-bit words from bitstring
            ListModeBitStringChunk = BitArray(bytes=chunk).bin
            ListModeWordArrayChunk = [ListModeBitStringChunk[i:i+WORD_BIT_LENGTH]
                                      for i in range(0, len(ListModeBitStringChunk), WORD_BIT_LENGTH)]

            # Build ADC and time data from bitwords
            for i in range(len(ListModeWordArrayChunk)):
                ListModeWord = ListModeWordArrayChunk[i]
                handleWord(ListModeWord)

            # Append data to .csv file
            # Cast to smaller data types to reduce file size
            # Will cause issue if ADC value exceeds 2^15 - 1 = 32,767
            df = DataFrame(data={'ADC': growingADCArray,
                                 'TimeStamp': growingTimeStampArray})
            df = df.astype({'ADC': 'int16', 'TimeStamp': 'float64'})
            df.to_csv(outDataFile, mode='a', header=header, index=False)
            header = False  # update to not write header in future loops

            # Update tqdm progress bar
            pbar.update(1)

        pbar.close()

    # Now that large data arrays are created, do meta-analysis
    # Determine acquisition start time and hardware delays from CONNECTIONS stamps
    # Note that these are preserved and do not follow the save/dump structure
    # However, I think they are not big contributors to the data storage
    if not len(growingC1Array) == len(growingC2Array) == len(growingC3Array):
        print("WARNING: Incomplete UMCBI timestamps found, check data file. Skipping acquisition timing.")
    elif not len(growingC1Array) == len(growingHDWArray):
        print("WARNING: Unpaired UMCBI/hardware timestamps found, check data file. Skipping acquisition timing.")
    else:
        ConnTimeT0 = parseCONNECTIONS(
            growingC1Array[0], growingC2Array[0], growingC3Array[0])

        UMCBI_timedeltas = []
        for i in trange(len(growingC1Array), desc='Constructing UMCBI timestamps'):
            c1, c2, c3 = growingC1Array[i], growingC2Array[i], growingC3Array[i]
            UMCBI_datetime = parseCONNECTIONS(c1, c2, c3)
            UMCBI_diff = (UMCBI_datetime - ConnTimeT0).total_seconds()
            UMCBI_timedeltas.append(UMCBI_diff)

        print('Calculating hardware offset statistics...')
        hardwareOffsets = UMCBI_timedeltas - np.array(growingHDWArray)
        meanHardwareOffset = np.mean(hardwareOffsets)
        varHardwareOffset = np.std(hardwareOffsets)
        spreadHardwareOffset = (np.min(hardwareOffsets),
                                np.max(hardwareOffsets))

        # Save metadata as separate file
        print('Saving metadata:')
        with open(outDataFile[:-4]+'_metaData.txt', 'a') as f:
            f.write('First UMCBI TimeStamp: {}\n'.format(ConnTimeT0))
            f.write('Hardware Response Time: {} +/- {} s {} (min, max)\n'.format(
                meanHardwareOffset, varHardwareOffset, spreadHardwareOffset
            ))
