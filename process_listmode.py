import sys
import datetime
from tqdm import trange
from bitstring import BitArray
from pandas import DataFrame
import numpy as np

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
    try:
        inListModeFile = sys.argv[1]
        outDataFile = sys.argv[2]
    except IndexError:
        raise TypeError('Missing positional arguments: infile, outfile')

    with open(inListModeFile, mode='rb') as f:
        fContent = f.read()

    processHeader(fContent)

    # Get 32-bit words from bitstring
    ListModeData = fContent[HEADER_BYTE_LENGTH:]
    ListModeBitString = BitArray(bytes=ListModeData).bin
    ListModeWordArray = [ListModeBitString[i:i+WORD_BIT_LENGTH]
                         for i in trange(0, len(ListModeBitString), WORD_BIT_LENGTH,
                                         desc='Loading words from bitstring')]

    # Build ADC and time data from bitwords
    for i in trange(len(ListModeWordArray), desc='Building data from bitwords'):
        ListModeWord = ListModeWordArray[i]
        handleWord(ListModeWord)

    # Determine acquisition start time and hardware delays from CONNECTIONS stamps
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

    print('Saving data:')
    df = DataFrame(data={'ADC': growingADCArray,
                   'TimeStamp': growingTimeStampArray})
    df.to_csv(outDataFile, index=False)
