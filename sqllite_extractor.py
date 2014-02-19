#!/usr/bin/env python
import sys
import struct
import re
import sqlite3 as sqlite
import varint_decoding

castingGroups = {
    "string": ["string", "char", "varchar", "current_time", "current_date", "current_timestamp", "text"],
    "int": ["smallint", "tinyint", "int", "bigint", "integer"],
    "blob": ["blob"],
    'no-type': ['string']
}

castingDump = {
    "string": lambda x: "".join(a.encode(typeOfEncoding) for a in x),
    "int": lambda x: ord(x),
    "blob": lambda x: "".join(a.encode("hex") for a in x)
}


dictOfEncoding = {
    1: 'utf-8',
    2: 'utf-16le',
    3: 'utf-16be'
}


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

    def disable(self):
        self.HEADER = ''
        self.OKBLUE = ''
        self.OKGREEN = ''
        self.WARNING = ''
        self.FAIL = ''
        self.ENDC = ''


dictOfPayloadTypes = {
    0: 'NULL',
    1: '8bit integer',
    2: '16bit integer',
    3: '24bit integer',
    4: '32bit integer',
    5: '48bit integer',
    6: '64bit integer',
    7: '64bit float',
    8: 'constant 0',
    9: 'constant 1',
    10: 'reserved',
    11: 'BLOB',
    12: 'string'
}

listOfStopWords = [
    'NOT',
    'NULL',
    'PRIMARY',
    'KEY',
    'AUTOINCREMENT',
    'UNIQUE',
    'DEFAULT',
    'REFERENCES'
]

computableLengthTypes = [
    "string",
    "blob",
    "no-type"
]

blobSaving="/tmp/SQLliteExtractor/extracted_blob"


class Table:

    def __init__(self, name, columns, root):
        patt = re.compile(r'\([\d]+\)')
        self.name = name
        self.rootPageNumber = root
        s = re.search(r'\((.*)\)', columns.split(
            name)[1])  # remove initial "(" and final ")" )
        s = s.group(1)
        listOfColumns = [el.strip().split() for el in s.split(",")]
        self.listOfColumns = []
        print bcolors.OKBLUE + "Found Table name: %s" % (self.name) + bcolors.ENDC
        listOfColumns = [col for col in listOfColumns if col[0] != "UNIQUE"]
        for column in listOfColumns:
            col = column[:]
            for piece in col:
                if piece in listOfStopWords:
                    column = column[:column.index(piece)]
            if len(column) == 1:
                column.append("NO-TYPE")
            else:
                column[1] = patt.sub("", column[1])
                for key, val in castingGroups.iteritems():
                    if column[1] in val:
                        column[1] = key
                        break
                else:
                    print bcolors.WARNING + "I can't find a value for Column Name %s" % (column[0]) + bcolors.ENDC
                    print bcolors.WARNING + "Assigning string as default" + bcolors.ENDC
                    print bcolors.WARNING + "Please update the dictionary on the top of the file with the right type" + bcolors.ENDC
                    column[1] = "string"

            dic = {"name": column[0], "type": column[1]}
            self.listOfColumns.append(dic)

    def addRootPage(self, page):
        self.rootPage = page

    def addAllPages(self, dictOfPages):
        def checkAllLeaves(listOfPages, dictOfPages):
            lstIntPages = []
            for index, page in enumerate(listOfPages):
                if dictOfPages[page].typeOfPage == 5:
                    lstIntPages.append((page, index))
            return lstIntPages

        lstPages = self.rootPage.getCellContentForInteriorPage()
        done = False
        while not done:
            lstIntPages = checkAllLeaves(lstPages, dictOfPages)
            if not lstIntPages:
                done = True
            for tup in lstIntPages:
                lstPages.remove(tup[1])
                lstPages += self.tup[0].getCellContentForInteriorPage()
        self.listOfPages = lstPages

    def estimateLengthFields(self):
        conn = sqlite.connect(sys.argv[1])
        c = conn.cursor()
        sql = "SELECT ? FROM " + self.name
        for fields in self.listOfColumns:
            typ = fields["type"].lower()
            if typ == "current_time":
                fields["averageSize"] = 8
            elif typ == "current_date":
                fields["averageSize"] = 10
            elif typ == "current_timestamp":
                fields["averageSize"] = 19
            elif typ in computableLengthTypes:
                sql = "SELECT " + fields["name"] + " FROM " + self.name
                c.execute(sql)
                res = c.fetchall()
                totSize = 0
                try:
                    totSize = reduce(lambda x, y: x + y, [len(el[
                                                          0]) for el in res if el[0] is not None], 0)
                    fields["averageSize"] = totSize / len(res)
                except TypeError:
                    pass
                except ZeroDivisionError:
                    self.isempty = True

    def __str__(self):

        st = "Table name: " + self.name + "\nRoot page Number: " + \
            str(self.rootPageNumber) + "\nList Of Columns:\n"
        for el in self.listOfColumns:
            st += "\t- name: %s type: %s" % (el["name"], el["type"])
            try:
                st += " average size: %d\n" % (el["averageSize"])
            except KeyError:
                st += "\n"
        try:
            st += "List Of Pages:\n"
            for el in self.listOfPages:
                st += "\t - page Number: %d\n" % (el)
        except AttributeError:
            st += "\t - No pages for The Table"
        return st


class Page:

    def __init__(self, data, num):
        self.data = data
        self.pageNumber = num
        self.stringData = ""
        for car in data:
            self.stringData += struct.unpack(">c", car)[0]
        self.typeOfPage = ord(struct.unpack(">c", data[0])[0])
        self.firstFreeBlock = struct.unpack(">H", data[1:3])[0]
        self.numOfCells = struct.unpack(">H", data[3:5])[0]
        self.firstCell = struct.unpack(">H", data[5:7])[0]

    def getCellContentForInteriorPage(self):
        address = self.firstCell
        listOfPages = []
        for i in xrange(self.numOfCells):
            pageNumber = struct.unpack(">I", self.data[address:address + 4])[0]
            address += 4
            keyID, next = varint_decoding.decodeVarint(
                self.data[address:], 0)
            address = address + next
            listOfPages.append(pageNumber)
        return listOfPages

    def printCellContentForFreePage(self):
        initialAddress = self.firstCell
        print bcolors.HEADER + "starting from address %d" % (initialAddress) + bcolors.ENDC
        for i in xrange(self.numOfCells):
            print bcolors.OKBLUE + "Cell %d:\n\tinitialAddress: %d" % (i, initialAddress) + bcolors.ENDC
            sizeOfCell, next = varint_decoding.decodeVarint(
                self.data[initialAddress:], 0)
            print bcolors.OKBLUE + "\tSize of cell: %d" % (sizeOfCell) + bcolors.ENDC
            keyCell, next = varint_decoding.decodeVarint(
                self.data[initialAddress:], next)
            print bcolors.OKBLUE + "\tRow key: %d" % (keyCell) + bcolors.ENDC
            increaseInAddress = sizeOfCell + next  # fixed
            payloadHeaderLength, next = varint_decoding.decodeVarint(
                self.data[initialAddress:], next)
            payloadHeaderLength -= 1
            print bcolors.OKBLUE + "\tPayloadHeader Length: %d" % (payloadHeaderLength) + bcolors.ENDC
            nextPayloadHeaderLength = 0
            initialAddressForPayloadHeader = initialAddress + next
            listOfPayloads = []
            while nextPayloadHeaderLength < payloadHeaderLength:
                payloadType, nextPayloadHeaderLength = varint_decoding.decodeVarint(
                    self.data[initialAddressForPayloadHeader:], nextPayloadHeaderLength)
                if payloadType == 0:
                    payloadType = 0
                    payloadSize = 0
                elif payloadType == 1:
                    payloadType = 1
                    payloadSize = 1
                elif payloadType == 2:
                    payloadType = 2
                    payloadSize = 2
                elif payloadType == 3:
                    payloadType = 3
                    payloadSize = 3
                elif payloadType == 4:
                    payloadType = 4
                    payloadSize = 4
                elif payloadType == 5:
                    payloadType = 5
                    payloadSize = 6
                elif payloadType == 6:
                    payloadType = 6
                    payloadSize = 8
                elif payloadType == 7:
                    payloadType = 7
                    payloadSize = 8
                elif payloadType == 8:
                    payloadType = 8
                    payloadSize = 0
                elif payloadType == 9:
                    payloadType = 9
                    payloadSize = 1
                elif payloadType == 10 or payloadType == 11:
                    payloadType = 10
                    payloadSize = 0
                elif payloadType >= 12 and payloadType % 2 == 0:
                    payloadSize = (payloadType - 12) / 2
                    payloadType = 11
                elif payloadType >= 13 and payloadType % 2 == 1:
                    payloadSize = (payloadType - 13) / 2
                    payloadType = 12
                listOfPayloads.append((dictOfPayloadTypes[
                                      payloadType], payloadSize))
            AddressData = initialAddressForPayloadHeader + payloadHeaderLength
            print bcolors.OKBLUE + "Content: " + bcolors.ENDC
            for el in listOfPayloads:
                print bcolors.OKGREEN + "\t\t %s of length %d" % (el[0], el[1]) + bcolors.ENDC
                print bcolors.OKGREEN + self.data[AddressData:AddressData + el[1]].encode("hex") + bcolors.ENDC
                AddressData += el[1]
            initialAddress += increaseInAddress  # sizeOfCell does not include the size of sizeOfCell and keyCell -> found in next

    def getListOfFreeBlocks(self):
        listOfFreeBlocks = []
        freeBlock = self.firstFreeBlock
        while freeBlock != 0:
            nextFreeBlock = struct.unpack(
                ">H", self.data[freeBlock:freeBlock + 2])[0]
            sizeOfFreeBlock = struct.unpack(
                ">H", self.data[freeBlock + 2:freeBlock + 4])[0]
            dic = {"size": sizeOfFreeBlock, "content": self.data[
                freeBlock:freeBlock + sizeOfFreeBlock]}
            listOfFreeBlocks.append(dic)
            freeBlock = nextFreeBlock
        return listOfFreeBlocks

    def printPage(self):
        print bcolors.OKGREEN + ' '.join(x.encode('hex') for x in self.stringData) + bcolors.ENDC

    def printInfo(self):
        print bcolors.OKBLUE + "Page number %d" % (self.pageNumber) + bcolors.ENDC
        print bcolors.OKBLUE + "\tType: %d" % (self.typeOfPage) + bcolors.ENDC
        print bcolors.OKBLUE + "\tFirst Free block: %d" % (self.firstFreeBlock) + bcolors.ENDC
        print bcolors.OKBLUE + "\tNumber Of Cells: %d" % (self.numOfCells) + bcolors.ENDC
        print bcolors.OKBLUE + "\tFirst cell at offset %d" % (self.firstCell) + bcolors.ENDC


def analyzeFreeBlock(tab, freeBlock):
    listOfCells = []
    minSizeOfCell = len(tab.listOfColumns) + 2
    sizeOfCell = minSizeOfCell
    endBlock = freeBlock["size"]
    totAdded = 0
    print bcolors.HEADER + "Analyzing Blocks:" + bcolors.ENDC
    while endBlock > sizeOfCell:
        possibleStop = struct.unpack(">H", freeBlock["content"][
                                     endBlock - sizeOfCell:endBlock - sizeOfCell + 2])[0]
        if possibleStop == sizeOfCell + 2 + totAdded:  # +2 because of the size of the cell itself
            print bcolors.OKGREEN + "Extracting a cell of size %d" % (sizeOfCell) + bcolors.ENDC
            print bcolors.OKGREEN + "Content: " + "\\x".join(x.encode('hex') for x in freeBlock["content"][endBlock + 2 - sizeOfCell:endBlock]) + bcolors.ENDC
            dic = {"size": sizeOfCell, "content": freeBlock[
                "content"][endBlock + 2 - sizeOfCell:endBlock]}
            listOfValues, done = analyzeFreeCells(tab, dic)
            if not done:
                sizeOfCell += 1
            else:
                listOfCells.append(dic)
                endBlock = endBlock - sizeOfCell - 2
                sizeOfCell = minSizeOfCell
                totAdded += possibleStop
                dic["listOfValues"] = listOfValues
        else:
            sizeOfCell += 1
    return listOfCells


def analyzeFreeCells(tab, cell):
    def askForNewValues(listOfColumns, cell):
        listOfParameters = []
        print "Analyzing this cell:"
        print bcolors.OKBLUE + "Size: %d Content: %s" % (cell["size"]-2, "\\x".join(x.encode('hex') for x in cell["content"])) + bcolors.ENDC
        print "Give me new sizes for each column"
        offset = 0
        index=0
        for column in listOfColumns:
            newParam = {"ColName": column["name"], "ColType": column["type"]}
            print bcolors.OKBLUE + "\tColumn name: %s Type: %s" % (column["name"], column["type"]) + bcolors.ENDC
            done = False
            while not done:
                newVal = int(raw_input("New Size (don't include initial \'\\x01\' for strings):   "))
                if newVal + offset >= cell["size"] or cell["size"]<newVal+offset+len(listOfColumns)-index:
                    print "Size out of a cell, repeat"
                else:
                    if column["type"] == "string":
                        try:
                            castingDump[column["type"]](cell["content"][offset:newVal + offset])
                            done = True
                        except:
                            print "This string can not be decoded, choose another size"
                    else:
                        done = True
            offset += newVal
            index+=1
            newParam["ColSize"] = newVal
            listOfParameters.append(newParam)
        return listOfParameters

    def printResults(listOfParameters, cell):
        listOfValues = []
        offset = 0
        for column in listOfParameters:
            print bcolors.OKBLUE + "\tColumn name: %s Type: %s Size: %d" % (column["ColName"], column["ColType"], column["ColSize"]) + bcolors.ENDC
            if cell["content"][offset] == "\x01" and column["ColType"] == "string":  # strings seem all to begin with \x01
                offset += 1
            val = {"name": column["ColName"], "type": column[
                "ColType"], "value": cell["content"][offset:column["ColSize"] + offset]}
            print bcolors.OKGREEN + "\t\tValue: %s" % (castingDump[column["ColType"]](val["value"])) + bcolors.ENDC
            print bcolors.OKGREEN +"\t\t" + "\\x".join(x.encode('hex') for x in val["value"]) + bcolors.ENDC
            if column["ColType"]=="blob": #save file
                f=open(blobSaving, "wb")
                f.write(x)
                print "blob saved to %s" %(blobSaving)
                f.close()
            listOfValues.append(val)
            offset += column["ColSize"]
        return listOfValues

    listOfValues = []
    print bcolors.HEADER + "Analyzing cell:" + bcolors.ENDC
    print bcolors.OKBLUE + "Size: %d Content: %s" % (cell["size"]-2, "\\x".join(x.encode('hex') for x in cell["content"])) + bcolors.ENDC
    print bcolors.HEADER + "Preliminary Estimation:" + bcolors.ENDC
    listOfParameters = []
    for column in tab.listOfColumns:
        dicOfParameters = {"ColName": column[
            "name"], "ColType": column["type"]}
        try:
            avg = column["averageSize"]
        except KeyError:
            avg = 1
        dicOfParameters["ColSize"] = avg
        listOfParameters.append(dicOfParameters)
    listOfValues = printResults(listOfParameters, cell)
    done = False
    while not done:
        # third option: stop this and come back to free block for reaching a
        # new block
        dec = raw_input(
            "It seems legit?\nDefault: Accept the values\n[N,n]: Use new values for decoding row\n[q,Q]: Look for another row\nChoice:   ")
        if dec == "n" or dec == "N":
            listOfParameters = askForNewValues(tab.listOfColumns, cell)
            listOfValues = printResults(listOfParameters, cell)
        elif dec == "Q" or dec == "q":
            return [], False
        else:
            done = True
    return listOfValues, True


if len(sys.argv) != 2:
    print "usage: %s <database>" %(sys.argv[0])
    sys.exit(1)

# Discover the tables inside the database
try:
    conn = sqlite.connect(sys.argv[1])
    c = conn.cursor()
    sql = "select name, sql, rootpage from sqlite_master where type='table'"
    c.execute(sql)
    res = c.fetchall()
    listOfTables = [Table(el[0], el[1], el[2]) for el in res]
except sqlite.OperationalError, e:
    print bcolors.FAIL + "PANIC: impossible to open db" + bcolors.ENDC

# Open the file, read all the pages
f = open(sys.argv[1], "rb")
try:
    byte = f.read(100)
except:
    print bcolors.FAIL + "PANIC: impossible to open file", sys.argv[1] + bcolors.ENDC
    sys.exit(1)
pageSize = struct.unpack(">H", byte[16:18])[0]
if pageSize == 1:  # SPECIAL: if pageSize = 1, it means 65536
    pageSize = 65536
unusableSpace = ord(struct.unpack(">c", byte[20])[0])
usableSpacePerPage = pageSize - unusableSpace
# totalNumberOfPages is calculated from these field from sqlite 3.7,
# otherwise from size of file
totalNumberOfPages = struct.unpack(">L", byte[28:32])[0]
if struct.unpack(">L", byte[24:28]) != struct.unpack(">L", byte[92:96]) or not totalNumberOfPages:
    totalNumberOfPages = sys.getsizeof(sys.argv[1] / pageSize)
numberOfFirstFreePage = struct.unpack(">L", byte[32:36])[0]
totalNumberOfFreePages = struct.unpack(">L", byte[36:40])[0]
typeOfEncoding = dictOfEncoding[struct.unpack(">i", byte[56:60])[0]]
s = f.read(pageSize - 100)  # consume rest of the page

print bcolors.OKGREEN + "total number of pages: " + str(totalNumberOfPages) + bcolors.ENDC
print bcolors.OKGREEN + "Size Of pages: %d" % (pageSize) + bcolors.ENDC
print bcolors.OKGREEN + "total number of free pages: " + str(totalNumberOfFreePages) + " starting from page: " + str(numberOfFirstFreePage) + bcolors.ENDC

dictOfPages = {}
# load all the pages in a single dictionary
for page in xrange(2, totalNumberOfPages + 1):
    dictOfPages[page] = Page(f.read(pageSize), page)

# add the root page for each table
for tab in listOfTables:
    tab.addRootPage(dictOfPages[tab.rootPageNumber])

# for each table, we need a list with all pages belonging to it
for tab in listOfTables:
    if tab.rootPage.typeOfPage == 5:  # it's a index page, we have to discover which are the pages forming this table
        tab.addAllPages(dictOfPages)

for tab in listOfTables:
    tab.estimateLengthFields()


for tab in listOfTables:
    if not hasattr(tab, "isempty"):
        print tab
        listOfFreeBlocks = tab.rootPage.getListOfFreeBlocks()
        try:
            for page in tab.listOfPages:
                listOfFreeBlocks += tab.dictOfPages[page].getListOfFreeBlocks()
        except AttributeError:
            pass
        if not listOfFreeBlocks:
            print "no free blocks present for table %s" %(tab.name)
        for el in listOfFreeBlocks:
            l = analyzeFreeBlock(tab, el)
            print bcolors.OKGREEN + "Restored %d cells" % (len(l)) + bcolors.ENDC
            for el in l:
                print bcolors.OKGREEN + "\t Size: %d" % (el["size"]) + bcolors.ENDC
                for val in el["listOfValues"]:
                    print bcolors.OKGREEN + "\t\t Column Name: %s Column Type: %s Column Value: %s" % (val["name"], val["type"], castingDump[val["type"]](val["value"]))

listOfFreePointers = []
if numberOfFirstFreePage != 0:
    freePage = dictOfPages[numberOfFirstFreePage]
    while freePage != 0:
        s = freePage.data
        print "This is a free Trunk page"
        nextfreePage = struct.unpack(">L", s[0:4])[0]
        print "Next free Trunk page: %d" % (nextfreePage)
        numberOfFreePointers = struct.unpack(">L", s[4:8])[0]
        print "Number of free page pointers present here: %d" % (numberOfFreePointers)
        start = 8
        for i in xrange(2, numberOfFreePointers + 2):
            el = (struct.unpack(">L", s[start:start + 4])[0])
            print "Free page: %d" % (el)
            listOfFreePointers.append(el)
            start += 4
        freePage.printPage()
        try:
            freePage = dictOfPages[nextfreePage]
        except KeyError:
            freePage = 0

for page in listOfFreePointers:
    print "Elements in free leaf pages:"
    dictOfPages[page].printInfo()
    dictOfPages[page].printCellContentForFreePage()
