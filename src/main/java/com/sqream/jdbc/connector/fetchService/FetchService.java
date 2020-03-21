package com.sqream.jdbc.connector.fetchService;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.ConnException;

public interface FetchService {
    void process(int rowAmount) throws ConnException;

    BlockDto getBlock() throws ConnException;

    boolean isClosed();
}
