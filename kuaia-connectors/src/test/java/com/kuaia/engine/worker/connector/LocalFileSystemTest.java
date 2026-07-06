package com.kuaia.engine.worker.connector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalFileSystemTest {
    @TempDir
    Path tempDir;

    private final KuaiaFileSystem fs = new LocalFileSystem();

    @Test
    void existsReflectsPresenceOfFilesAndDirectories() throws Exception {
        Path file = tempDir.resolve("intro.md");
        Files.write(file, "hello".getBytes(StandardCharsets.UTF_8));

        assertTrue(fs.exists(file.toString()));
        assertTrue(fs.exists(tempDir.toString()));
        assertFalse(fs.exists(tempDir.resolve("missing.md").toString()));
    }

    @Test
    void isDirectoryDistinguishesDirectoriesFromRegularFiles() throws Exception {
        Path file = tempDir.resolve("intro.md");
        Files.write(file, "hello".getBytes(StandardCharsets.UTF_8));

        assertTrue(fs.isDirectory(tempDir.toString()));
        assertFalse(fs.isDirectory(file.toString()));
    }

    @Test
    void listReturnsRegularFilesRecursivelyAndSkipsDirectories() throws Exception {
        Files.createDirectories(tempDir.resolve("nested"));
        Path intro = tempDir.resolve("intro.md");
        Path guide = tempDir.resolve("nested/guide.txt");
        Files.write(intro, "intro".getBytes(StandardCharsets.UTF_8));
        Files.write(guide, "guide".getBytes(StandardCharsets.UTF_8));

        List<String> listed = fs.list(tempDir.toString());

        assertEquals(2, listed.size());
        assertTrue(listed.contains(intro.toString()), "expected " + intro + " in " + listed);
        assertTrue(listed.contains(guide.toString()), "expected " + guide + " in " + listed);
        assertFalse(listed.contains(tempDir.resolve("nested").toString()), "directories must not be listed");
    }

    @Test
    void readAllBytesReadsTheWholeObject() throws Exception {
        byte[] payload = "café résumé".getBytes(StandardCharsets.UTF_8);
        Path file = tempDir.resolve("body.txt");
        Files.write(file, payload);

        assertArrayEquals(payload, fs.readAllBytes(file.toString()));
    }

    @Test
    void acceptsFileSchemeUrisAsWellAsBarePaths() throws Exception {
        byte[] payload = "scheme body".getBytes(StandardCharsets.UTF_8);
        Path file = tempDir.resolve("body.txt");
        Files.write(file, payload);
        String barePath = file.toString();
        String fileUri = file.toUri().toString();

        assertTrue(fileUri.startsWith("file://"), "expected a file:// URI but was " + fileUri);
        assertTrue(fs.exists(barePath));
        assertTrue(fs.exists(fileUri));
        assertFalse(fs.isDirectory(fileUri));
        assertArrayEquals(payload, fs.readAllBytes(barePath));
        assertArrayEquals(payload, fs.readAllBytes(fileUri));
    }

    @Test
    void listAcceptsFileSchemeUriRoot() throws Exception {
        Path intro = tempDir.resolve("intro.md");
        Files.write(intro, "intro".getBytes(StandardCharsets.UTF_8));

        List<String> listed = fs.list(tempDir.toUri().toString());

        assertEquals(1, listed.size());
        assertTrue(listed.contains(intro.toString()), "expected " + intro + " in " + listed);
    }
}
