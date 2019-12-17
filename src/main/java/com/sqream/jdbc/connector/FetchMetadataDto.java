package com.sqream.jdbc.connector;

public class FetchMetadataDto {

    private int newRowsFetched;
    private int[] sizes;

    public FetchMetadataDto(int newRowsFetched, int[] sizes) {
        this.newRowsFetched = newRowsFetched;
        this.sizes = sizes;
    }

    public int getNewRowsFetched() {
        return newRowsFetched;
    }

    public int colAmount() {
        return this.sizes.length;
    }

    public int getSizeByIndex(int index) {
        return this.sizes[index];
    }
}
