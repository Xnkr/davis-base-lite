package com.davisbase.lite.utils;

import com.davisbase.lite.DavisBaseBinaryFile;
import com.davisbase.lite.metadata.InteriorRecord;
import com.davisbase.lite.metadata.PageType;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class BPlusTreeImpl {
    RandomAccessFile binaryFile;
    int rootPageNo;
    String tableName;

    public BPlusTreeImpl(RandomAccessFile file, int rootPageNo, String tableName) {
        this.binaryFile = file;
        this.rootPageNo = rootPageNo;
        this.tableName = tableName;
    }



    public List<Integer> getAllLeaves() throws IOException {

        List<Integer> leafPages = new ArrayList<>();
        binaryFile.seek(rootPageNo * DavisBaseBinaryFile.pageSize);
        PageType rootPageType = PageUtil.getPage(binaryFile.readByte());
        if (rootPageType == PageType.LEAF) {
            leafPages.add(rootPageNo);
        } else {
            addLeaves(rootPageNo, leafPages);
        }

        return leafPages;

    }

    private void addLeaves(int interiorPageNo, List<Integer> leafPages) throws IOException {
        PageUtil interiorPageType = new PageUtil(binaryFile, interiorPageNo);
        for (InteriorRecord leftPage : interiorPageType.leftChildren) {
            if (PageUtil.getPageType(binaryFile, leftPage.leftChildPageNo) == PageType.LEAF) {
                if (!leafPages.contains(leftPage.leftChildPageNo))
                    leafPages.add(leftPage.leftChildPageNo);
            } else {
                addLeaves(leftPage.leftChildPageNo, leafPages);
            }
        }

        if (PageUtil.getPageType(binaryFile, interiorPageType.rightPage) == PageType.LEAF) {
            if (!leafPages.contains(interiorPageType.rightPage))
                leafPages.add(interiorPageType.rightPage);
        } else {
            addLeaves(interiorPageType.rightPage, leafPages);
        }

    }

    public List<Integer> getAllLeaves(ConditionParser condition) throws IOException {
        return getAllLeaves();
    }

    public static int getPageNoForInsert(RandomAccessFile file, int rootPageNo) {
        PageUtil rootPageType = new PageUtil(file, rootPageNo);
        if (rootPageType.pageType != PageType.LEAF)
            return getPageNoForInsert(file, rootPageType.rightPage);
        else
            return rootPageNo;

    }

    public int getPageNo(int rowId, PageUtil pageType) {
        if (pageType.pageType == PageType.LEAF)
            return pageType.pageNo;

        int index = binarySearch(pageType.leftChildren, rowId, 0, pageType.noOfCells - 1);

        if (rowId < pageType.leftChildren.get(index).rowId) {
            return getPageNo(rowId, new PageUtil(binaryFile, pageType.leftChildren.get(index).leftChildPageNo));
        } else {
            if (index + 1 < pageType.leftChildren.size())
                return getPageNo(rowId, new PageUtil(binaryFile, pageType.leftChildren.get(index + 1).leftChildPageNo));
            else
                return getPageNo(rowId, new PageUtil(binaryFile, pageType.rightPage));


        }
    }

    private int binarySearch(List<InteriorRecord> values, int searchValue, int start, int end) {

        if (end - start <= 2) {
            int i;
            for (i = start; i < end; i++) {
                if (values.get(i).rowId < searchValue)
                    continue;
                else
                    break;
            }
            return i;
        } else {

            int mid = (end - start) / 2 + start;
            if (values.get(mid).rowId == searchValue)
                return mid;

            if (values.get(mid).rowId < searchValue)
                return binarySearch(values, searchValue, mid + 1, end);
            else
                return binarySearch(values, searchValue, start, mid - 1);

        }

    }
}
