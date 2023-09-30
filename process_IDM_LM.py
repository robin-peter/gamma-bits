import sys
from tqdm import trange
from bitstring import BitArray
from pandas import DataFrame

FINE_TIME_SAMPLE = 200e-9  # 200 ns per fine time sample increment
COARSE_TIME_SAMPLE = 10e-3  # 10 ms per coarse time sample increment
WORD_BIT_LENGTH = 32
HEADER_BYTE_LENGTH = 256

globalCoarseTime = 0  # initialize start time if not given before a data word

growingADCArray = []  # in ADC CHA
growingTimeStampArray = []  # in seconds


def handleWord(word):
    # len(32) string of 0s and 1s
    encoderByte = word[-8:]
    if encoderByte[:2] == '11':  # ADC word
        handleDataWord(word)
    elif encoderByte[:2] == '10':  # RT word
        handleRTWord(word)
    elif encoderByte[:2] == '01':  # LT word
        pass  # can be adapted to handle these in the future
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


def processHeader(fileContent):
    pass  # accomodate header later


if __name__ == '__main__':
    try:
        inListModeFile = sys.argv[1]
        outDataFile = sys.argv[2]
    except IndexError:
        raise TypeError('Missing positional arguments: infile, outfile')

    with open(inListModeFile, mode='rb') as f:
        fContent = f.read()

    processHeader(fContent)
    ListModeData = fContent[HEADER_BYTE_LENGTH:]
    ListModeBitString = BitArray(bytes=ListModeData).bin
    ListModeWordArray = [ListModeBitString[i:i+WORD_BIT_LENGTH]
                         for i in trange(0, len(ListModeBitString), WORD_BIT_LENGTH,
                                         desc='Loading words from bitstring')]

    for i in trange(len(ListModeWordArray), desc='Building data from bitwords'):
        ListModeWord = ListModeWordArray[i]
        handleWord(ListModeWord)

    print('Saving data:')
    df = DataFrame(data={'ADC': growingADCArray, 'TimeStamp': growingTimeStampArray})
    df.to_csv(outDataFile, index=False)