package com.kuaia.engine.worker.connector.v2;

import com.kuaia.engine.worker.connector.FileSource;
import com.kuaia.engine.worker.connector.LocalSource.RecordConsumer;
import com.kuaia.engine.worker.connector.LocalSource.RecordErrorConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileSourceAdapterTest {
    @TempDir
    Path tempDir;

    @Test
    void enumeratesSplitsFromNonEmptyCsvRecordCountAndReadersHonorRanges() throws Exception {
        Path csv = tempDir.resolve("documents.csv");
        Files.write(csv, String.join("\n",
                "id,content",
                "1,Alpha",
                "",
                "2,Beta",
                "3,Gamma").getBytes(StandardCharsets.UTF_8));
        FileSourceAdapter adapter = new FileSourceAdapter(new FileSource(csv), "file-0", 2);

        adapter.open();
        List<SourceSplit> splits = adapter.enumerateSplits();

        assertEquals(2, splits.size());
        assertSplit(splits.get(0), "file-0-part-0", 1L, 2L);
        assertSplit(splits.get(1), "file-0-part-1", 3L, 3L);

        List<Long> firstSeqIds = new ArrayList<>();
        int firstRead = adapter.createReader(splits.get(0)).readFrom(
                0L,
                (seqId, row) -> firstSeqIds.add(seqId),
                (seqId, error) -> false);
        List<Long> secondSeqIds = new ArrayList<>();
        int secondRead = adapter.createReader(splits.get(1)).readFrom(
                0L,
                (seqId, row) -> secondSeqIds.add(seqId),
                (seqId, error) -> false);

        assertEquals(2, firstRead);
        assertEquals(Arrays.asList(1L, 2L), firstSeqIds);
        assertEquals(1, secondRead);
        assertEquals(Arrays.asList(3L), secondSeqIds);
        adapter.close();
    }

    @Test
    void headerOnlyCsvEnumeratesOneFullRangeSplitAndReadsZeroRows() throws Exception {
        Path csv = tempDir.resolve("empty-documents.csv");
        Files.write(csv, "id,content\n".getBytes(StandardCharsets.UTF_8));
        FileSourceAdapter adapter = new FileSourceAdapter(new FileSource(csv), "file-0", 2);

        adapter.open();
        List<SourceSplit> splits = adapter.enumerateSplits();
        List<Long> seqIds = new ArrayList<>();
        int read = adapter.createReader(splits.get(0)).readFrom(
                0L,
                (seqId, row) -> seqIds.add(seqId),
                (seqId, error) -> false);

        assertEquals(1, splits.size());
        assertSplit(splits.get(0), "file-0", 1L, Long.MAX_VALUE);
        assertEquals(0, read);
        assertEquals(new ArrayList<>(), seqIds);
        adapter.close();
    }

    @Test
    void readersUseBoundedFileRangeForSplitReads() throws Exception {
        Path csv = tempDir.resolve("bounded-documents.csv");
        Files.write(csv, String.join("\n",
                "id,content",
                "1,Alpha",
                "2,Beta",
                "3,Gamma").getBytes(StandardCharsets.UTF_8));
        TrackingFileSource source = new TrackingFileSource(csv);
        FileSourceAdapter adapter = new FileSourceAdapter(source, "file-0", 2);

        adapter.open();
        List<SourceSplit> splits = adapter.enumerateSplits();
        List<Long> seqIds = new ArrayList<>();
        int read = adapter.createReader(splits.get(0)).readFrom(
                0L,
                (seqId, row) -> seqIds.add(seqId),
                (seqId, error) -> false);

        assertEquals(2, read);
        assertEquals(Arrays.asList(1L, 2L), seqIds);
        assertEquals(1, source.rangeReads);
        assertEquals(Arrays.asList(2L), source.rangeEndSeqIds);
        adapter.close();
    }

    @Test
    void rejectsNonPositiveRowsPerSplit() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new FileSourceAdapter(new FileSource(tempDir.resolve("missing.csv")), "file-0", 0));
    }

    private void assertSplit(SourceSplit split, String splitId, long startSeqInclusive, long endSeqInclusive) {
        assertEquals(splitId, split.getSplitId());
        assertEquals(startSeqInclusive, split.getStartSeqInclusive());
        assertEquals(endSeqInclusive, split.getEndSeqInclusive());
    }

    private static class TrackingFileSource extends FileSource {
        private int rangeReads;
        private final List<Long> rangeEndSeqIds = new ArrayList<>();

        TrackingFileSource(Path path) {
            super(path);
        }

        @Override
        public int readRange(
                long lastCheckpointSeq,
                long endSeqInclusive,
                RecordConsumer consumer,
                RecordErrorConsumer errorConsumer) throws Exception {
            rangeReads++;
            rangeEndSeqIds.add(endSeqInclusive);
            return super.readRange(lastCheckpointSeq, endSeqInclusive, consumer, errorConsumer);
        }
    }
}
