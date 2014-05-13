/**
 * Copyright 2013, 2014 MIT Libraries
 * Licensed under: http://www.apache.org/licenses/LICENSE-2.0
 */

package edu.mit.lib.bagit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import static edu.mit.lib.bagit.Bag.*;
/**
 * Loader is a Bag builder class that interprets bag serializations and other
 * manfestations as Bags. It also exposes methods allowing clients to fill
 * bag 'holes' (fetch.txt contents), and updates the bag to reflect this.
 *
 * See README for sample invocations and API description.
 *
 * @author richardrodgers
 */
public class Loader {
    // base directory of bag
    private Path base;
    // checksum algorithm used in bag
    private String csAlg;
    // map of unresolved fetch.txt files
    private final ConcurrentMap<String, String> payloadRefMap = new ConcurrentHashMap<>();
    // manifest writer
    private LoaderWriter manWriter;
   
   /**
     * Returns a new Loader (bag loader) instance using passed
     * file (loose directory or archive file)
     *
     * @param file the base directory or archive file from which to extract the bag
     */
    public Loader(Path file) throws IOException {
        if (file == null || Files.notExists(file)) {
            throw new IOException("Missing or nonexistent bag file");
        }
        // is it an archive file? If so,inflate into bag
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
    }

    /**
     * Returns a new Loader (bag loader) instance using passed I/O stream
     * and format with bag in a temporary directory location
     *
     * @param in the input stream containing the serialized bag
     * @param format the expected serialization format
     */
    public Loader(InputStream in, String format) throws IOException {
        this(null, in, format);
    }

     /**
     * Returns a new Loader (bag loader) instance using passed I/O stream
     * and format with bag in the passed directory location
     *
     * @param parent the parent directory into which to extract the bag directory
     * @param in the input stream containing the serialized bag
     * @param format the expected serialization format
     */
    public Loader(Path parent, InputStream in, String format) throws IOException {
        Path theParent = (parent != null) ? parent : Files.createTempDirectory("bagparent");
        inflate(theParent, in, format);
    }

    /**
     * Returns the checksum algortihm used in bag manifests.
     *
     * @return algorithm the checksum algorithm
     */
    public String csAlgorithm() throws IOException {
        if (csAlg == null) {
            csAlg = Bag.csAlgorithm(base); 
        }
        return csAlg;
    }

    /**
     * Returns sealed Bag from Loader. Sealed Bags cannot be serialized.
     *
     * @return bag the loaded sealed Bag instance
     */
    public Bag seal() throws IOException {
        finish();
        return new Bag(this.base, true);
    }

    /**
     * Returns Bag from Loader
     *
     * @return bag the loaded Bag instance
     */
    public Bag load() throws IOException {
        finish();
        return new Bag(this.base, false);
    }

    private void finish() throws IOException {
        // if manWriter is non-null, some payload files were fetched.
        if (manWriter != null) {
            manWriter.close();
            // Update fetch.txt - remove if all holes plugged, else filter
            Path refFile = bagFile(REF_FILE);
            List<String> refLines = bufferFile(refFile);
            if (payloadRefMap.size() > 0) {
                // now reconstruct fetch.txt filtering out those resolved
                try (OutputStream refOut = Files.newOutputStream(refFile)) {
                    for (String refline : refLines) {
                        String[] parts = refline.split(" ");
                        if (payloadRefMap.containsKey(parts[2])) {
                            refOut.write(refline.getBytes(ENCODING));
                        } 
                    }
                }
            }
            // update tagmanifest with new manifest checksum, fetch stuff
            String sfx = csAlgorithm() + ".txt";
            Path tagManFile = bagFile(TAGMANIF_FILE + sfx);
            List<String> tmLines = bufferFile(tagManFile);
            // now recompute manifest checksum
            String manCS = checksum(bagFile(MANIF_FILE + sfx), csAlgorithm());
            // likewise fetch.txt if it's still around
            String fetchCS = Files.exists(refFile) ? checksum(bagFile(MANIF_FILE + sfx), csAlgorithm()) : null;
            // recreate tagmanifest with new checksums
            try (OutputStream tagManOut = Files.newOutputStream(tagManFile)) {
                for (String tline : tmLines) {
                    String[] parts = tline.split(" ");
                    if (parts[1].startsWith(MANIF_FILE)) {
                        tagManOut.write((manCS + " " + MANIF_FILE + sfx + "\n").getBytes(ENCODING));
                    } else if (parts[1].startsWith(REF_FILE)) {
                        if (fetchCS != null) {
                            tagManOut.write((fetchCS + " " + REF_FILE + sfx + "\n").getBytes(ENCODING));
                        }
                    } else {
                        tagManOut.write(tline.getBytes(ENCODING));
                    }
                }
            }
        }
    }

    private List<String> bufferFile(Path file) throws IOException {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                lines.add(line + "\n");
            }
        }
        Files.delete(file);
        return lines;
    }

    /**
     * Returns a map of payload files to fetch URLs (ie contents of fetch.txt).
     *
     * @return refMap the map of payload files to fetch URLs
     */
    public Map<String, String> payloadRefs() throws IOException {
        Path refFile = bagFile(REF_FILE);
        if (payloadRefMap.isEmpty() && Files.exists(refFile)) {
            // load initial data
            payloadRefMap.putAll(Bag.payloadRefs(refFile));
        }
        return payloadRefMap;
    }

    /**
     * Resolves a payload reference with passed stream content.
     *
     * @param relPath the bag-relative path to the payload file
     * @param is the content input stream for payload
     */
    public void resolveRef(String relPath, InputStream is) throws IOException {
        // various checks - is ref known?
        if (! payloadRefMap.containsKey(relPath)) {
            throw new IOException ("Unknown payload reference: " + relPath);
        }
        if (Files.exists(bagFile(relPath))) {
            throw new IllegalStateException("Payload file already exists at: " + relPath);
        }
        // wrap stream in digest stream
        try (DigestInputStream dis = new DigestInputStream(is, MessageDigest.getInstance(csAlgorithm()))) {
            Files.copy(dis, bagFile(relPath));
            // record checksum
            manifestWriter().writeLine(toHex(dis.getMessageDigest().digest()) + " " + relPath);
            // remove from map
            payloadRefMap.remove(relPath);
        } catch (NoSuchAlgorithmException nsaE) {
            throw new IOException("no algorithm: " + csAlg);
        }
    }

    private Path bagFile(String name) {
        return base.resolve(name);
    }

    private Path dataFile(String name) throws IOException {
        // all user-defined files live in payload area - ie. under 'data'
        Path dataFile = bagFile(DATA_DIR).resolve(name);
        // create needed dirs
        Path parentFile = dataFile.getParent();
        if (! Files.isDirectory(parentFile)) {
            Files.createDirectories(parentFile);
        }
        return dataFile;
    }

    // lazy initialization of manifest writer
    private synchronized LoaderWriter manifestWriter() throws IOException {
        if (manWriter == null) {
            String sfx = csAlgorithm().toLowerCase() + ".txt";
            manWriter = new LoaderWriter(bagFile(MANIF_FILE + sfx), null, true, null);
        }
        return manWriter;
    }

    class LoaderWriter extends LoaderOutputStream {
    
        private LoaderWriter(Path file, String brPath, boolean append, LoaderWriter tailWriter) throws IOException {
            super(file, brPath, append, tailWriter);
        }

        public void writeLine(String line) throws IOException {
            write((line + "\n").getBytes(ENCODING));
        }
    }

    // wraps output stream in digester, and records results with tail writer
    class LoaderOutputStream extends OutputStream {

        private final String relPath;
        private final OutputStream out;
        private final DigestOutputStream dout;
        private final LoaderWriter tailWriter;

        private LoaderOutputStream(Path file, String relPath, boolean append, LoaderWriter tailWriter) throws IOException {
            OpenOption opt = append ? StandardOpenOption.APPEND : StandardOpenOption.READ;
            try {
                out = Files.newOutputStream(file, opt);
                dout = new DigestOutputStream(out, MessageDigest.getInstance(csAlg));
                this.relPath = (relPath != null) ? relPath : file.getFileName().toString();
                this.tailWriter = tailWriter;
            } catch (NoSuchAlgorithmException nsae) {
                throw new IOException("no such algorithm: " + csAlg);
            }
        }

        @Override
        public void write(int b) throws IOException {
            dout.write(b);
        }

        @Override
        public void close() throws IOException {
            dout.flush();
            out.close();
            if (tailWriter != null) {
                tailWriter.writeLine(toHex(dout.getMessageDigest().digest()) + " " + relPath);
            }
        }
    }

    // inflate compressesd archive in base directory
    private void inflate(Path parent, InputStream in, String fmt) throws IOException {
        switch (fmt) {
            case "zip" :
                try (ZipInputStream zin = new ZipInputStream(in)) {
                    ZipEntry entry = null;
                    while((entry = zin.getNextEntry()) != null) {
                        if (base == null) {
                            base = parent.resolve(entry.getName().substring(0, entry.getName().indexOf("/")));
                        }
                        Path outFile = base.getParent().resolve(entry.getName());
                        Files.createDirectories(outFile.getParent());
                        Files.copy(zin, outFile);
                    }
                }
                break;
            case "tgz" :
                try (TarArchiveInputStream tin = new TarArchiveInputStream(
                                                 new GzipCompressorInputStream(in))) {
                    TarArchiveEntry tentry = null;
                    while((tentry = tin.getNextTarEntry()) != null) {
                        if (base == null) {
                            base = parent.resolve(tentry.getName().substring(0, tentry.getName().indexOf("/")));
                        }
                        Path outFile = parent.resolve(tentry.getName());
                        Files.createDirectories(outFile.getParent());
                        Files.copy(tin, outFile);
                    }
                }
                break;
            default:
                throw new IOException("Unsupported archive format: " + fmt);                            
        }
    }

    private String checksum(Path file, String csAlg) throws IOException {
        byte[] buf = new byte[2048];
        int num = 0;
        // wrap stream in digest stream
        try (InputStream is = Files.newInputStream(file);
             DigestInputStream dis = new DigestInputStream(is, MessageDigest.getInstance(csAlg))) {
            while (num != -1) {
                num = dis.read(buf);
            }
            return toHex(dis.getMessageDigest().digest());
        } catch (NoSuchAlgorithmException nsaE) {
            throw new IOException("no algorithm: " + csAlg);
        }
    }
}
