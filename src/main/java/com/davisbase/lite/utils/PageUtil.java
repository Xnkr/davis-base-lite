package com.davisbase.lite.utils;

import com.davisbase.lite.DavisBaseBinaryFile;
import com.davisbase.lite.metadata.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

import static java.lang.System.out;

public class PageUtil {

    public PageType pageType;
    short noOfCells = 0;
    public int pageNo;
    short contentStartOffset;
    public int rightPage;
    public int parentPageNo;
    private List<Row> records;
    boolean refreshTableRecords = false;
    long pageStart;
    int lastRowId;
    int availableSpace;
    RandomAccessFile binaryFile;
    List<InteriorRecord> leftChildren;
    private Map<Integer, Row> recordsMap;

    private static final Map<Byte, PageType> pageMap = new HashMap<>();

    static {
        for (PageType s : PageType.values())
            pageMap.put(s.getValue(), s);
    }

    public boolean isRoot() {
        return parentPageNo == -1;
    }

    public PageUtil(RandomAccessFile file, int pageNo) {
        try {
            this.pageNo = pageNo;

            this.binaryFile = file;
            lastRowId = 0;
            pageStart = DavisBaseBinaryFile.pageSize * pageNo;
            binaryFile.seek(pageStart);
            pageType = PageUtil.getPage(binaryFile.readByte());
            binaryFile.readByte();
            noOfCells = binaryFile.readShort();
            contentStartOffset = binaryFile.readShort();
            availableSpace = contentStartOffset - 0x10 - (noOfCells * 2);

            rightPage = binaryFile.readInt();

            parentPageNo = binaryFile.readInt();

            binaryFile.readShort();

            if (pageType == PageType.LEAF)
                fillTableRecords();
            if (pageType == PageType.INTERIOR)
                fillLeftChildren();

        } catch (IOException ex) {
            System.out.println("Exception while reading the page " + ex.getMessage());
        }
    }

    public static PageType getPage(byte value) {
        return pageMap.get(value);
    }

    public static PageType getPageType(RandomAccessFile file, int pageNo) throws IOException {
        int pageStart = DavisBaseBinaryFile.pageSize * pageNo;
        file.seek(pageStart);
        return PageUtil.getPage(file.readByte());
    }


    public static int addNewPage(RandomAccessFile file, PageType pageType, int rightPage, int parentPageNo) {
        try {
            int pageNo = Long.valueOf((file.length() / DavisBaseBinaryFile.pageSize)).intValue();
            file.setLength(file.length() + DavisBaseBinaryFile.pageSize);
            file.seek(DavisBaseBinaryFile.pageSize * pageNo);
            file.write(pageType.getValue());
            file.write(0x00);
            file.writeShort(0);
            file.writeShort((short) (DavisBaseBinaryFile.pageSize));

            file.writeInt(rightPage);

            file.writeInt(parentPageNo);

            return pageNo;
        } catch (IOException ex) {
            System.out.println("Exception while adding new page" + ex.getMessage());
            return -1;
        }
    }

    public void addNewColumn(Column column) throws IOException {
        try {
            addTableRow(DavisBaseBinaryFile.davisbaseColumns, Arrays.asList(new Attribute(DataType.TEXT, column.getTable().getTableName()),
                    new Attribute(DataType.TEXT, column.getColumnName()),
                    new Attribute(DataType.TEXT, column.getDataType().toString()),
                    new Attribute(DataType.SMALLINT, column.getOrdinalPosition().toString()),
                    new Attribute(DataType.TEXT, column.isNullable() ? "YES" : "NO"),
                    column.isPrimaryKey() ? new Attribute(DataType.TEXT, "PRI") : new Attribute(DataType.NULL, "NULL"),
                    new Attribute(DataType.TEXT, column.isUnique() ? "YES" : "NO")));
        } catch (Exception e) {
            System.out.println("Could not add column");
            out.println(e);
        }
    }


    public int addTableRow(String tableName, List<Attribute> attributes) throws IOException {
        List<Byte> colDataTypes = new ArrayList<Byte>();
        List<Byte> recordBody = new ArrayList<Byte>();

        Table metaData = null;
        if (DavisBaseBinaryFile.isDataStoreInitialized) {
            metaData = new Table();
            metaData.setTableName(tableName);
            metaData.loadTable();
            if (!metaData.validateInsert(attributes))
                return -1;
        }

        for (Attribute attribute : attributes) {

            recordBody.addAll(Arrays.asList(attribute.fieldValueByte));


            if (attribute.dataType == DataType.TEXT) {
                colDataTypes.add(Integer.valueOf(DataType.TEXT.getValue() + (new String(attribute.fieldValueStr).length())).byteValue());
            } else {
                colDataTypes.add(attribute.dataType.getValue());
            }
        }

        lastRowId++;


        short payLoadSize = Integer.valueOf(recordBody.size() +
                colDataTypes.size() + 1).shortValue();


        List<Byte> recordHeader = new ArrayList<>();

        recordHeader.addAll(Arrays.asList(ByteUtil.shortToBytes(payLoadSize)));
        recordHeader.addAll(Arrays.asList(ByteUtil.intToBytes(lastRowId)));
        recordHeader.add(Integer.valueOf(colDataTypes.size()).byteValue());
        recordHeader.addAll(colDataTypes);

        addNewPageRecord(recordHeader.toArray(new Byte[recordHeader.size()]),
                recordBody.toArray(new Byte[recordBody.size()])
        );

        refreshTableRecords = true;
        if (DavisBaseBinaryFile.isDataStoreInitialized) {
            metaData.setRowCount(metaData.getRowCount() + 1);
            metaData.updateMetaData();
        }
        return lastRowId;
    }

    private void addNewPageRecord(Byte[] recordHeader, Byte[] recordBody) throws IOException {

        if (recordHeader.length + recordBody.length + 4 > availableSpace) {
            try {
                handleTableOverFlow();
            } catch (IOException e) {
                System.out.println("Exception while handleTableOverFlow");
            }
        }

        short cellStart = contentStartOffset;


        short newCellStart = Integer.valueOf((cellStart - recordBody.length - recordHeader.length - 2)).shortValue();
        binaryFile.seek(pageNo * DavisBaseBinaryFile.pageSize + newCellStart);


        binaryFile.write(ByteUtil.Bytestobytes(recordHeader));


        binaryFile.write(ByteUtil.Bytestobytes(recordBody));

        binaryFile.seek(pageStart + 0x10 + (noOfCells * 2));
        binaryFile.writeShort(newCellStart);

        contentStartOffset = newCellStart;

        binaryFile.seek(pageStart + 4);
        binaryFile.writeShort(contentStartOffset);

        noOfCells++;
        binaryFile.seek(pageStart + 2);
        binaryFile.writeShort(noOfCells);

        availableSpace = contentStartOffset - 0x10 - (noOfCells * 2);

    }

    public void setParent(int parentPageNo) throws IOException {
        binaryFile.seek(DavisBaseBinaryFile.pageSize * pageNo + 0x0A);
        binaryFile.writeInt(parentPageNo);
        this.parentPageNo = parentPageNo;
    }


    public void setRightPageNo(int rightPageNo) throws IOException {
        binaryFile.seek(DavisBaseBinaryFile.pageSize * pageNo + 0x06);
        binaryFile.writeInt(rightPageNo);
        this.rightPage = rightPageNo;
    }

    private void handleTableOverFlow() throws IOException {
        int newRightLeafPageNo = addNewPage(binaryFile, pageType, -1, -1);
        if (pageType == PageType.LEAF) {
            if (parentPageNo == -1) {
                createLeaf(newRightLeafPageNo);
            } else {
                PageUtil parentPage = new PageUtil(binaryFile, parentPageNo);
                parentPageNo = parentPage.addLeftTableChild(pageNo, lastRowId);
                parentPage.setRightPageNo(newRightLeafPageNo);
                setRightPageNo(newRightLeafPageNo);
                PageUtil newLeafPage = new PageUtil(binaryFile, newRightLeafPageNo);
                newLeafPage.setParent(parentPageNo);
                shiftPage(newLeafPage);
            }
        } else {
            createLeaf(newRightLeafPageNo);
        }
    }

    private void createLeaf(int newRightLeafPageNo) throws IOException {
        int newParentPageNo = addNewPage(binaryFile, PageType.INTERIOR,
                newRightLeafPageNo, -1);
        setRightPageNo(newRightLeafPageNo);
        setParent(newParentPageNo);
        PageUtil newParentPage = new PageUtil(binaryFile, newParentPageNo);
        newParentPageNo = newParentPage.addLeftTableChild(pageNo, lastRowId);
        newParentPage.setRightPageNo(newRightLeafPageNo);
        PageUtil newLeafPage = new PageUtil(binaryFile, newRightLeafPageNo);
        newLeafPage.setParent(newParentPageNo);
        shiftPage(newLeafPage);
    }

    private void shiftPage(PageUtil newPage) {
        pageType = newPage.pageType;
        noOfCells = newPage.noOfCells;
        pageNo = newPage.pageNo;
        contentStartOffset = newPage.contentStartOffset;
        rightPage = newPage.rightPage;
        parentPageNo = newPage.parentPageNo;
        leftChildren = newPage.leftChildren;
        records = newPage.records;
        pageStart = newPage.pageStart;
        availableSpace = newPage.availableSpace;
    }

    private int addLeftTableChild(int leftChildPageNo, int rowId) throws IOException {
        for (InteriorRecord intRecord : leftChildren) {
            if (intRecord.rowId == rowId)
                return pageNo;
        }
        if (pageType == PageType.INTERIOR) {
            List<Byte> recordHeader = new ArrayList<>();
            List<Byte> recordBody = new ArrayList<>();

            recordHeader.addAll(Arrays.asList(ByteUtil.intToBytes(leftChildPageNo)));
            recordBody.addAll(Arrays.asList(ByteUtil.intToBytes(rowId)));

            addNewPageRecord(recordHeader.toArray(new Byte[recordHeader.size()]),
                    recordBody.toArray(new Byte[recordBody.size()]));
        }
        return pageNo;

    }

    public List<Row> getPageRecords() {

        if (refreshTableRecords)
            fillTableRecords();

        refreshTableRecords = false;

        return records;
    }

    private void fillTableRecords() {
        short payLoadSize = 0;
        byte noOfcolumns = 0;
        records = new ArrayList<Row>();
        recordsMap = new HashMap<>();
        try {
            for (short i = 0; i < noOfCells; i++) {
                binaryFile.seek(pageStart + 0x10 + (i * 2));
                short cellStart = binaryFile.readShort();
                if (cellStart == 0)
                    continue;
                binaryFile.seek(pageStart + cellStart);

                payLoadSize = binaryFile.readShort();
                int rowId = binaryFile.readInt();
                noOfcolumns = binaryFile.readByte();

                if (lastRowId < rowId) lastRowId = rowId;

                byte[] colDatatypes = new byte[noOfcolumns];
                byte[] recordBody = new byte[payLoadSize - noOfcolumns - 1];

                binaryFile.read(colDatatypes);
                binaryFile.read(recordBody);

                Row record = new Row(i, rowId, cellStart, colDatatypes, recordBody);
                records.add(record);
                recordsMap.put(rowId, record);
            }
        } catch (IOException ex) {
            System.out.println("Exception while filling records from the page " + ex.getMessage());
        }
    }

    private void fillLeftChildren() {
        try {
            leftChildren = new ArrayList<>();

            int leftChildPageNo = 0;
            int rowId = 0;
            for (int i = 0; i < noOfCells; i++) {
                binaryFile.seek(pageStart + 0x10 + (i * 2));
                short cellStart = binaryFile.readShort();
                if (cellStart == 0)
                    continue;
                binaryFile.seek(pageStart + cellStart);

                leftChildPageNo = binaryFile.readInt();
                rowId = binaryFile.readInt();
                leftChildren.add(new InteriorRecord(rowId, leftChildPageNo));
            }
        } catch (IOException ex) {
            System.out.println("Exception while filling records from the page " + ex.getMessage());
        }

    }

    public static int getRootPageNo(RandomAccessFile binaryfile) {
        int rootpage = 0;
        try {
            for (int i = 0; i < binaryfile.length() / DavisBaseBinaryFile.pageSize; i++) {
                binaryfile.seek(i * DavisBaseBinaryFile.pageSize + 0x0A);
                int a = binaryfile.readInt();

                if (a == -1) {
                    return i;
                }
            }
            return rootpage;
        } catch (Exception e) {
            out.println("Exception while getting root page no ");
        }
        return -1;
    }

    public void updateRow(Row record, int ordinalPosition, Byte[] newValue) throws IOException {
        binaryFile.seek(pageStart + record.getRecordOffset() + 7);
        int valueOffset = 0;
        for (int i = 0; i < ordinalPosition; i++) {

            valueOffset += DataTypeUtil.getLength((byte) binaryFile.readByte());
        }

        binaryFile.seek(pageStart + record.getRecordOffset() + 7 + record.getDataTypes().length + valueOffset);
        binaryFile.write(ByteUtil.Bytestobytes(newValue));

    }
}
