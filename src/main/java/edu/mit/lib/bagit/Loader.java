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
 
    /**
     * Returns a Bag instance from passed file
     * (loose directory or archive file)
     *
     * @param file the base directory or archive file from which to extract the bag
     * @return bag the bag in the directory or archive
     * @throws IOException if error loading bag
     */
    public static Bag load(Path file) throws IOException {
        return loadFile(file, false);
    }

    /**
     * Returns a Bag instance using passed I/O stream
     * and format with bag in a temporary directory location
     *
     * @param in the input stream containing the serialized bag
     * @param format the expected serialization format
     * @return bag the bag in the temporary directory
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
     * Returns a sealed Bag instance from passed file
     * (loose directory or archive file)
     *
     * @param file the base directory or archive file from which to extract the bag
     * @return bag the bag in the directory or archive
     * @throws IOException if error loading bag
     */
    public static Bag seal(Path file) throws IOException {
        return loadFile(file, true);
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

    private static Bag loadFile(Path file, boolean sealed) throws IOException {
        if (file == null || Files.notExists(file)) {
            throw new IOException("Missing or nonexistent bag file");
        }
        // is it an archive file? If so,inflate into bag
        Path base = null;
        String baseName = file.getFileName().toString();
        int sfxIdx = baseName.lastIndexOf(".");
        String suffix = (sfxIdx != -1) ? baseName.substring(sfxIdx + 1) : null;
        if (! Files.isDirectory(file) && suffix != null &&
            (suffix.equals(DFLT_FMT) || suffix.equals(TGZIP_FMT))) {
            String dirName = baseName.substring(0, sfxIdx);
             base = file.getParent().resolve(dirName);
             Files.createDirectories(base.resolve(DATA_DIR));
             inflate(file.getParent(), Files.newInputStream(file), suffix);
             // remove archive original
             Files.delete(file);
         } else {
             base = file;
         }
        return new Bag(base, sealed);
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

    /*
    private String checksum(Path file, String csAlg) throws IOException {
        byte[] buf = new byte[2048];
        int num = 0;
        // wrap stream in digest stream
        try (InputStream is = Files.newInputStream(file);
             DigestInputStream dis = new DigestInputStream(is, MessageDigest.getInstance(csAlgoCode(csAlg)))) {
            while (num != -1) {
                num = dis.read(buf);
            }
            return toHex(dis.getMessageDigest().digest());
        } catch (NoSuchAlgorithmException nsaE) {
            throw new IOException("no algorithm: " + csAlg);
        }
    }
    */
}
