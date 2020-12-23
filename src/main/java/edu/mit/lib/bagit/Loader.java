/**
 * Copyright 2013, 2014 MIT Libraries
 * SPDX-Licence-Identifier: Apache-2.0
 */

package edu.mit.lib.bagit;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import static edu.mit.lib.bagit.Bag.*;
/**
 * Loader is a class with static methods to interpret archive files and other
 * forms as representations or serializations of Bags. 
 *
 * See README for sample invocations and API description.
 *
 * @author richardrodgers
 */
public class Loader {

    private static Set<String> knownFmts = Set.of(DFLT_FMT, "ZIP", TGZIP_FMT, "gz");
    private static byte[] zipSig = {0x50, 0x4b};
    private static byte[] tgzSig = {0x1f, (byte)0x8b};
    private static Set<byte[]> knownSigs = Set.of(zipSig, tgzSig);
 
    /**
     * Returns a Bag instance from passed file, which can be
     * either a loose directory or an archive file. If archive file,
     * create bag directory as sibling (same parent directory),
     * and remove original artifact.
     *
     * @param file base directory or archive file from which to extract the bag
     * @return bag the bag in the directory
     * @throws IOException if error loading bag
     */
    public static Bag load(Path file) throws IOException {
        return loadFile(null, file, false, true);
    }

    /**
     * Returns a Bag instance from passed file, which can be
     * either a loose directory or an archive file. Bag directory created
     * as child of passed parent directory, and original artifact is untouched.
     *
     * @param parent the parent directory into which the bag directory extracted
     * @param file the base directory or archive file from which to extract the bag
     * @return bag the bag in the directory
     * @throws IOException if error loading bag
     */
    public static Bag load(Path parent, Path file) throws IOException {
        return loadFile(parent, file, false, false);
    }

    /**
     * Returns a Bag instance using passed I/O stream
     * and format with bag in a temporary directory location
     *
     * @param in the input stream containing the serialized bag
     * @param format the expected serialization format
     * @return bag the bag in a temporary directory
     * @throws IOException if error loading stream
     */
    public static Bag load(InputStream in, String format) throws IOException {
        return loadStream(null, in, format, false);
    }

    /**
     * Returns a Bag instance using passed I/O stream
     * and format with bag in the passed directory location
     *
     * @param parent the parent directory into which to extract the bag directory
     * @param in the input stream containing the serialized bag
     * @param format the expected serialization format
     * @return bag the bag in the temporary directory
     * @throws IOException if error loading stream
     */
    public static Bag load(Path parent, InputStream in, String format) throws IOException {
        return loadStream(parent, in, format, false);
    }

    /**
     * Returns a sealed Bag instance from passed file, which can be
     * either a loose directory or an archive file. If archive file,
     * create bag directory as sibling (same parent directory),
     * and remove original artifact.
     *
     * @param file the base directory or archive file from which to extract the bag
     * @return bag the bag in the directory or archive
     * @throws IOException if error loading bag
     */
    public static Bag seal(Path file) throws IOException {
        return loadFile(null, file, true, true);
    }

    /**
     * Returns a sealed Bag instance from passed file, which can be
     * either a loose directory or an archive file. Bag directory created
     * as child of passed parent directory, and original artifact is untouched.
     *
     * @param parent the parent directory into which the bag directory extracted
     * @param file the base directory or archive file from which to extract the bag
     * @return bag the bag in the directory
     * @throws IOException if error loading bag
     */
    public static Bag seal(Path parent, Path file) throws IOException {
        return loadFile(parent, file, false, false);
    }

     /**
     * Returns a sealed Bag instance using passed I/O stream
     * and format with bag in a temporary directory location
     *
     * @param in the input stream containing the serialized bag
     * @param format the expected serialization format
     * @return bag the bag in the temporary directory
     * @throws IOException if error loading stream
     */
    public static Bag seal(InputStream in, String format) throws IOException {
        return loadStream(null, in, format, true);
    }

    /**
     * Returns a sealed Bag instance using passed I/O stream
     * and format with bag in the passed directory location
     *
     * @param parent the parent directory into which to extract the bag directory
     * @param in the input stream containing the serialized bag
     * @param format the expected serialization format
     * @return bag the bag in the temporary directory
     * @throws IOException if error loading stream
     */
    public static Bag seal(Path parent, InputStream in, String format) throws IOException {
        return loadStream(parent, in, format, false);
    }

    private static Bag loadFile(Path parent, Path file, boolean sealed, boolean cleanup) throws IOException {
        if (file == null || Files.notExists(file)) {
            throw new IOException("Missing or nonexistent bag file");
        }
        // is it an archive file? If so,inflate into bag
        if (isReadableArchiveFile(file)) {
            var fileName = file.getFileName().toString();
            Path parentDir = parent != null ? parent : file.getParent();
            Path base = parentDir.resolve(baseName(fileName));
            Files.createDirectories(base.resolve(DATA_DIR));
            inflate(parentDir, Files.newInputStream(file), canonicalSuffix(fileName));
            // remove archive original
            if (cleanup) {
                Files.delete(file);
            }
            return new Bag(base, sealed);
        } else if (Files.isDirectory(file)) {
            return new Bag(file, sealed);
        } else {
            throw new IOException("Not a directory or supported readable archive format");
        }
    }

    private static String baseName(String fileName) {
        int idx = fileName.lastIndexOf(".");
        var stem = fileName.substring(0, idx);
        if (stem.endsWith(".tar")) {
            int idx2 = stem.lastIndexOf(".");
            return stem.substring(0, idx2);
        }
        return stem;
    }

    private static String canonicalSuffix(String fileName) {
        int sfxIdx = fileName.lastIndexOf(".");
        if (sfxIdx != -1) {
            var sfx = fileName.substring(sfxIdx + 1);
            if (sfx.equals("gz")) {
                return TGZIP_FMT; 
            }
            return sfx.toLowerCase();
        }
        return null;
    }

    private static boolean isReadableArchiveFile(Path file) {
        if (Files.isRegularFile(file)) {
            String baseName = file.getFileName().toString();
            int sfxIdx = baseName.lastIndexOf(".");
            if (sfxIdx > 0 && knownFmts.contains(baseName.substring(sfxIdx + 1))) {
                // peek inside
                try (InputStream in = Files.newInputStream(file)) {
                    var signature = in.readNBytes(2);
                    for (byte[] sig : knownSigs) {
                        if (Arrays.equals(sig, signature)) {
                            return true;
                        }
                    }
                } catch (IOException ioe) {}
            }
        }
        return false;
    }

    private static Bag loadStream(Path parent, InputStream in, String format, boolean sealed) throws IOException {
        Path theParent = (parent != null) ? parent : Files.createTempDirectory("bagparent");
        return new Bag(inflate(theParent, in, format), sealed);
    }

    // inflate compressesd archive in base directory
    private static Path inflate(Path parent, InputStream in, String fmt) throws IOException {
        Path base = null;
        switch (fmt) {
            case "zip" :
                try (ZipInputStream zin = new ZipInputStream(in)) {
                    ZipEntry entry;
                    while((entry = zin.getNextEntry()) != null) {
                        if (base == null) {
                            base = parent.resolve(entry.getName().substring(0, entry.getName().indexOf("/")));
                        }
                        Path outFile = base.getParent().resolve(entry.getName());
                        Files.createDirectories(outFile.getParent());
                        Files.copy(zin, outFile);
                        // Set file attributes to ZipEntry values
                        Files.setAttribute(outFile, "creationTime", entry.getCreationTime());
                        Files.setLastModifiedTime(outFile, entry.getLastModifiedTime());
                    }
                }
                break;
            case "tgz" :
                try (TarArchiveInputStream tin = new TarArchiveInputStream(
                                                 new GzipCompressorInputStream(in))) {
                    TarArchiveEntry tentry;
                    while((tentry = tin.getNextTarEntry()) != null) {
                        if (base == null) {
                            base = parent.resolve(tentry.getName().substring(0, tentry.getName().indexOf("/")));
                        }
                        Path outFile = parent.resolve(tentry.getName());
                        Files.createDirectories(outFile.getParent());
                        Files.copy(tin, outFile);
                        Files.setLastModifiedTime(outFile, FileTime.fromMillis(tentry.getLastModifiedDate().getTime()));
                    }
                }
                break;
            default:
                throw new IOException("Unsupported archive format: " + fmt);
        }
        return base;
    }
}
