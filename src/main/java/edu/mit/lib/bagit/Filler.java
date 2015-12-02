/**
 * Copyright 2013, 2014 MIT Libraries
 * Licensed under: http://www.apache.org/licenses/LICENSE-2.0
 */

package edu.mit.lib.bagit;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import static edu.mit.lib.bagit.Bag.*;
import static edu.mit.lib.bagit.Bag.MetadataName.*;
import static edu.mit.lib.bagit.Filler.EolRule.*;

/**
 * Filler is a builder class used to construct bags conformant to LC Bagit spec - version 0.97.
 * Filler objects serialize themselves to either a loose directory, a compressed archive file (supported
 * formats zip or tgz) or a stream, abiding by the serialization recommendations of the specification.
 *
 * See README for sample invocations and API description.
 *
 * @author richardrodgers
 */

public class Filler {

    // directory root of bag
    private Path base;
    // checksum algorithm
    private String csAlg;
    // automatic metadata generation set
    private Set<MetadataName> autogenNames;
    // total payload size
    private long payloadSize = 0L;
    // number of payload files
    private int payloadCount = 0;
    // manifest writers
    private FlatWriter tagWriter;
    private FlatWriter manWriter;
    // optional flat writers
    private Map<String, FlatWriter> writers;
    // optional bag streams
    private Map<String, BagOutputStream> streams;
    // has bag been built?
    private boolean built;
    // transient bag?
    private boolean transientBag;
    // line separator used by FlatWriters
    private final String lineSeparator;

    /**
     * Rule for assigning the EOL (line termination/separation)
     */
    public enum EolRule {
        /**
         * Use system-defined separator
         */
        SYSTEM,
        /**
         * Use Windows separators on Unix systems,
         * or vice versa
         */
        COUNTER_SYSTEM,
        /**
         * Use Unix new-line '\n' separators
         */
        UNIX,
        /**
         * Use Windows CR/LF separators
         */
        WINDOWS
    }

    /**
     * Returns a new Filler (bag builder) instance using
     * temporary directory to hold bag, default checksum
     * algorithm (MD5), and system-defined line separator
     *
     * @throws IOException if error creating bag
     */
    public Filler() throws IOException {
        this(null, null, SYSTEM);
    }

    /**
     * Returns a new Filler (bag builder) instance using passed
     * directory to hold bag, default checksum algorithm (MD5),
     * and system-defined line separator
     *
     * @param base the base directory in which to construct the bag
     * @throws IOException if error creating bag
     */
    public Filler(Path base) throws IOException {
        this(base, null, SYSTEM);
    }

    /**
     * Returns a new filler (bag builder) instances using passed directory
     * and checksum algorithm, and system-defined line separator
     *
     * @param base directory for bag - if null, create temporary directory
     * @param csAlgorithm checksum algorithm string - if null use default
     * @throws IOException if error creating bag
     */
    public Filler(Path base, String csAlgorithm) throws IOException {
        this(base, csAlgorithm, SYSTEM);
    }

    /**
     * Returns a new filler (bag builder) instances using passed directory,
     * checksum algorithm, and line separator for text files.
     *
     * @param base directory for bag - if null, create temporary directory
     * @param csAlgorithm checksum algorithm string - if null use default
     * @param eolRule line termination rule to use for generated text files. Values are:
     *            SYSTEM - use system-defined line termination
     *            COUNTER_SYSTEM - if on Windows, use Unix EOL, else reverse
     *            UNIX - use newline character line termination
     *            WINDOWS - use CR/LF line termination
     *
     * @throws IOException if error creating bag
     */
    public Filler(Path base, String csAlgorithm, EolRule eolRule) throws IOException {
        if (base != null) {
            this.base = base;
        } else {
            this.base = Files.createTempDirectory("bag");
            transientBag = true;
        }
        csAlg = (csAlgorithm != null) ? csAlgorithm : CS_ALGO;
        Path dirPath = bagFile(DATA_DIR);
        if (Files.notExists(dirPath)) {
            Files.createDirectories(dirPath);
        }
        // prepare manifest writers
        String sfx = csAlg.toLowerCase() + ".txt";
        tagWriter = new FlatWriter(bagFile(TAGMANIF_FILE + sfx), null, null, false);
        manWriter = new FlatWriter(bagFile(MANIF_FILE + sfx), null, tagWriter, true);
        writers = new HashMap<>();
        streams = new HashMap<>();
        // set up default auto-generated metadata
        autogenNames = new HashSet<>();
        autogenNames.add(BAGGING_DATE);
        autogenNames.add(BAG_SIZE);
        autogenNames.add(PAYLOAD_OXUM);
        autogenNames.add(BAG_SOFTWARE_AGENT);
        String sysEol = System.lineSeparator();
        switch (eolRule) {
            case SYSTEM: lineSeparator = sysEol; break;
            case UNIX: lineSeparator = "\n"; break;
            case WINDOWS: lineSeparator = "\r\n"; break;
            case COUNTER_SYSTEM: lineSeparator = "\n".equals(sysEol) ? "\r\n" : "\n"; break;
            default: lineSeparator = sysEol; break;
        }
    }

    private void buildBag() throws IOException {
        if (built) return;
        // if auto-generating any metadata, do so
        Iterator<MetadataName> agIter = autogenNames.iterator();
        while (agIter.hasNext()) {
            switch (agIter.next()) {
                 case BAGGING_DATE :
                     metadata(BAGGING_DATE, new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
                     break;
                 case BAG_SIZE :
                     metadata(BAG_SIZE, scaledSize(payloadSize, 0));
                     break;
                 case PAYLOAD_OXUM :
                     metadata(PAYLOAD_OXUM, String.valueOf(payloadSize) + "." + String.valueOf(payloadCount));
                     break;
                 case BAG_SOFTWARE_AGENT :
                     metadata(BAG_SOFTWARE_AGENT, "MIT BagIt Lib v:" + LIB_VSN);
                    break;
                 default : break;
            }
        }
        // close all optional writers' tag files
        Iterator<String> wIter = writers.keySet().iterator();
        while (wIter.hasNext()) {
            getWriter(wIter.next()).close();
        }
        // close all optional output streams
        Iterator<String> sIter = streams.keySet().iterator();
        while (sIter.hasNext()) {
            getStream(null, sIter.next(), null, true).close();
        }
        // close the manifest file
        manWriter.close();
        // write out bagit declaration file
        FlatWriter fwriter = new FlatWriter(bagFile(DECL_FILE), null, tagWriter, false);
        fwriter.writeLine("BagIt-Version: " + BAGIT_VSN);
        fwriter.writeLine("Tag-File-Character-Encoding: " + ENCODING);
        fwriter.close();
        // close tag manifest file of previous tag files
        tagWriter.close();
        built = true;
    }

    /**
     * Disables the automatic generation of metadata.
     * Normally generated: Bagging-Date, Bag-Size, Payload-Oxnum, Bag-Software-Agent
     *
     * @return Filler this Filler
     */
    public Filler noAutoGen() {
        return autoGen(new HashSet<MetadataName>());
    }

    /**
     * Assigns the set of automatically generated metadata identifed by their names.
     * Unknown or non-auto-assignable names will be ignored.
     *
     * @param names the set of metadata names
     * @return Filler this Filler
     */
    public Filler autoGen(Set<MetadataName> names) {
        autogenNames = names;
        return this;
    }

    /**
     * Adds a file to the payload at the root of the data
     * directory tree - convenience method when no payload hierarchy needed.
     *
     * @param topFile the file to add to the payload
     * @return Filler this Filler
     * @throws IOException if error reading/writing payload
     */
    public Filler payload(Path topFile) throws IOException {
        return payload(topFile.getFileName().toString(), topFile);
    }

    /**
     * Adds a file to the payload at the specified relative
     * path from the root of the data directory tree.
     *
     * @param relPath the relative path of the file
     * @param file the file path to add to the payload
     * @return Filler this Filler
     * @throws IOException if error reading/writing payload
     */
    public Filler payload(String relPath, Path file) throws IOException {
        Path payloadFile = dataFile(relPath);
        commitPayload(payloadFile, relPath, Files.newInputStream(file));
        // now rewrite payload attrs to original values
        BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
        Files.setAttribute(payloadFile, "creationTime", attrs.creationTime());
        Files.setLastModifiedTime(payloadFile, attrs.lastModifiedTime());
        return this;
    }

    /**
     * Adds the contents of the passed stream to the payload
     * at the specified relative path in the data directory tree.
     *
     * @param relPath the relative path of the file
     * @param is the input stream to read.
     * @return Filler this Filler
     * @throws IOException if error reading/writing payload
     */
    public Filler payload(String relPath, InputStream is) throws IOException {
        commitPayload(dataFile(relPath), relPath, is);
        return this;
    }

    private void commitPayload(Path payloadFile, String relPath, InputStream is) throws IOException {
      if (Files.exists(payloadFile)) {
          throw new IllegalStateException("Payload file already exists at: " + relPath);
      }
      // wrap stream in digest stream
      try (DigestInputStream dis =
          new DigestInputStream(is, MessageDigest.getInstance(csAlg))) {
          payloadSize += Files.copy(dis, payloadFile);
          payloadCount++;
          // record checksum
          manWriter.writeLine(toHex(dis.getMessageDigest().digest()) + " " + DATA_PATH + relPath);
      } catch (NoSuchAlgorithmException nsaE) {
          throw new IOException("no algorithm: " + csAlg);
      }
    }

    /**
     * Obtains manifest of bag contents.
     *
     * @return List manifest list
     */
    public List<String> getManifest() {
        return manWriter.getLines();
    }

    /**
     * Adds a reference URL to payload contents - ie. to the fetch.txt file.
     *
     * @param relPath the relative path of the resource
     * @param size the expected size in bytes of the resource
     * @param url the URL of the resource
     * @return Filler this Filler
     * @throws IOException if error reading/writing ref data
     */
    public Filler payloadRef(String relPath, long size, String url) throws IOException {
        FlatWriter refWriter = getWriter(REF_FILE);
        String sizeStr = (size > 0L) ? Long.toString(size) : "-";
        refWriter.writeLine(url + " " + sizeStr + " " + DATA_PATH + relPath);
        payloadSize += size;
        payloadCount++;
        return this;
    }

    /**
     * Obtains an output stream to a payload file at a relative path.
     *
     * @param relPath the relative path to the payload file
     * @return stream an output stream to payload file
     * @throws IOException if error reading/writing payload
     */
    public OutputStream payloadStream(String relPath) throws IOException {
        if (Files.exists(dataFile(relPath))) {
            throw new IllegalStateException("Payload file already exists at: " + relPath);
        }
        return getStream(dataFile(relPath), relPath, manWriter, true);
    }

    /**
     * Adds a tag (metadata) file at the specified relative
     * path from the root of the bag directory tree.
     *
     * @param relPath the relative path of the file
     * @param file the path of the tag file to add
     * @return Filler this Filler
     * @throws IOException if error reading/writing tag
     */
    public Filler tag(String relPath, Path file) throws IOException {
        return tag(relPath, Files.newInputStream(file));
    }

    /**
     * Adds the contents of the passed stream to a tag (metadata) file
     * at the specified relative path in the bag directory tree.
     *
     * @param relPath the relative path of the file
     * @param is the input stream to read.
     * @return Filler this Filler
     * @throws IOException if error reading/writing tag
     */
    public Filler tag(String relPath, InputStream is) throws IOException {
        // make sure tag files not written to payload directory
        if (relPath.startsWith(DATA_PATH)) {
            throw new IOException("Tag files not allowed in paylod directory");
        }
        if (Files.exists(bagFile(relPath))) {
            throw new IllegalStateException("Tag file already exists at: " + relPath);
        }
        // wrap stream in digest stream
        try (DigestInputStream dis =
             new DigestInputStream(is, MessageDigest.getInstance(csAlg))) {
            Files.copy(dis, tagFile(relPath));
            // record checksum
            tagWriter.writeLine(toHex(dis.getMessageDigest().digest()) + " " + relPath);
        } catch (NoSuchAlgorithmException nsaE) {
            throw new IOException("no algorithm: " + csAlg);
        }
        return this;
    }

    /**
     * Obtains an output stream to the tag file at a relative path.
     *
     * @param relPath the relative path to the tag file
     * @return stream an output stream to the tag file
     * @throws IOException if error reading/writing tag
     */
    public OutputStream tagStream(String relPath) throws IOException {
        if (Files.exists(tagFile(relPath))) {
            throw new IllegalStateException("Tag file already exists at: " + relPath);
        }
        return getStream(tagFile(relPath), relPath, tagWriter, false);
    }

    /**
     * Adds a reserved metadata property to the standard file
     * (bag-info.txt)
     *
     * @param name the property name
     * @param value the property value
     * @return filler this filler
     * @throws IOException if error writing metadata
     */
    public Filler metadata(MetadataName name, String value) throws IOException {
        return property(META_FILE, name.getName(), value);
    }

    /**
     * Adds a metadata property to the standard file
     * (bag-info.txt)
     *
     * @param name the property name
     * @param value the property value
     * @return filler this filler
     * @throws IOException if error writing metadata
     */
    public Filler metadata(String name, String value) throws IOException {
        return property(META_FILE, name, value);
    }

    /**
     * Adds a property to the passed property file.
     * Typically used for metadata properties in tag files.
     *
     * @param relPath the bag-relative path to the property file
     * @param name the property name
     * @param value the property value
     * @return filler this filler
     * @throws IOException if error writing property
     */
    public Filler property(String relPath, String name, String value) throws IOException {
        FlatWriter writer = getWriter(relPath);
        writer.writeProperty(name, value);
        return this;
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

    private Path tagFile(String name) throws IOException {
        // all user-defined tag files live anywhere in the bag
        Path tagFile = bagFile(name);
        // create needed dirs
        Path parentFile = tagFile.getParent();
        if (! Files.isDirectory(parentFile)) {
            Files.createDirectories(parentFile);
        }
        return tagFile;
    }

    private Path bagFile(String name) {
        return base.resolve(name);
    }

    private synchronized FlatWriter getWriter(String name) throws IOException {
        FlatWriter writer = writers.get(name);
        if (writer == null) {
            writer = new FlatWriter(bagFile(name), null, tagWriter, false);
            writers.put(name, writer);
        }
        return writer;
    }

    private BagOutputStream getStream(Path path, String name, FlatWriter tailWriter, boolean isPayload) throws IOException {
        BagOutputStream stream = streams.get(name);
        if (stream == null) {
            stream = new BagOutputStream(path, name, tailWriter, isPayload);
            streams.put(name, stream);
        }
        return stream;
    }

    class FlatWriter extends BagOutputStream {

        private final List<String> lines = new ArrayList<>();
        private final boolean record;

        private FlatWriter(Path file, String brPath, FlatWriter tailWriter, boolean record) throws IOException {
            super(file, brPath, tailWriter, false);
            this.record = record;
        }

        public void writeProperty(String key, String value) throws IOException {
            String prop = key + ": " + value;
            int offset = 0;
            while (offset < prop.length()) {
                int end = Math.min(prop.length() - offset, 80);
                if (offset > 0) {
                    write(SPACER.getBytes(ENCODING));
                }
                writeLine(prop.substring(offset, offset + end));
                offset += end;
            }
        }

        public void writeLine(String line) throws IOException {
            if (record) {
              lines.add(line);
            }
            byte[] bytes = (line + lineSeparator).getBytes(ENCODING);
            write(bytes);
        }

        public List<String> getLines() {
            return lines;
        }
    }

    // wraps output stream in digester, and records results with tail writer
    class BagOutputStream extends OutputStream {

        private final String relPath;
        private final OutputStream out;
        private final DigestOutputStream dout;
        private final FlatWriter tailWriter;
        private final boolean isPayload;
        private boolean closed = false;

        private BagOutputStream(Path file, String relPath, FlatWriter tailWriter, boolean isPayload) throws IOException {
            try {
                out = Files.newOutputStream(file);
                dout = new DigestOutputStream(out, MessageDigest.getInstance(csAlg));
                this.relPath = (relPath != null) ? relPath : file.getFileName().toString();
                this.tailWriter = tailWriter;
                this.isPayload = isPayload;
            } catch (NoSuchAlgorithmException nsae) {
                throw new IOException("no such algorithm: " + csAlg);
            }
        }

        @Override
        public void write(int b) throws IOException {
            dout.write(b);
        }

        @Override
        public synchronized void close() throws IOException {
            if (! closed) {
                dout.flush();
                out.close();
                if (tailWriter != null) {
                    String path = isPayload ? DATA_PATH + relPath : relPath;
                    tailWriter.writeLine(toHex(dout.getMessageDigest().digest()) + " " + path);
                }
                closed = true;
            }
        }
    }

    /**
     * Returns backing bag directory path.
     *
     * @return dir the bag directory path
     * @throws IOException if error reading bag
     */
    public Path toDirectory() throws IOException {
        buildBag();
        return base;
    }

    /**
     * Returns bag serialized as an archive file using default packaging (zip archive).
     *
     * @return path the bag archive package path
     * @throws IOException if error reading bag
     */
    public Path toPackage() throws IOException {
        return toPackage(DFLT_FMT);
    }

    /**
     * Returns bag serialized as an archive file using passed packaging format.
     * Supported formats: 'zip' - zip archive, 'tgz' - gzip compressed tar archive
     *  'zip.nt' and 'tgz.nt' the same except file time attributes suppressed
     *
     * @param format the package format ('zip', or 'tgz')
     * @return path the bag archive package path
     * @throws IOException if error reading bag
     */
    public Path toPackage(String format) throws IOException {
        return deflate(format);
    }

    /**
     * Returns bag serialized as an IO stream using default packaging (zip archive).
     * Bag is deleted when stream closed if temporary bag location used.
     *
     * @return file the bag archive package
     * @throws IOException if error reading bag
     */
    public InputStream toStream() throws IOException {
        return toStream(DFLT_FMT);
    }

    /**
     * Returns bag serialized as an IO stream using passed packaging format.
     * Bag is deleted when stream closed if temporary bag location used.
     * Supported formats: 'zip' - zip archive, 'tgz' - gzip compressed tar archive
     *
     * @param format the package format ('zip', or 'tgz')
     * @return file the bag archive package
     * @throws IOException if error reading bag
     */
    public InputStream toStream(String format) throws IOException {
        Path pkgFile = deflate(format);
        if (transientBag) {
            return new CleanupInputStream(Files.newInputStream(pkgFile), pkgFile);
        } else {
            return Files.newInputStream(pkgFile);
        }
    }

    class CleanupInputStream extends FilterInputStream {

        private Path file;

        public CleanupInputStream(InputStream in, Path file) {
            super(in);
            this.file = file;
        }

        @Override
        public void close() throws IOException {
            super.close();
            Files.delete(file);
        }
    }

    private void empty() throws IOException {
        deleteDir(base);
        Files.delete(base);
    }

    private void deleteDir(Path dirFile) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirFile)) {
            for (Path path : stream) {
                if (Files.isDirectory(path)) {
                    deleteDir(path);
                }
                Files.delete(path);
            }
        } catch (IOException ioE) {}
    }

    private Path deflate(String format) throws IOException {
        // deflate this bag in situ (in current directory) using given packaging format
        buildBag();
        int ndIdx = format.indexOf(".nt");
        String sfx = (ndIdx > 0) ? format.substring(0, ndIdx) : format;
        Path pkgFile = base.getParent().resolve(base.getFileName().toString() + "." + sfx);
        deflate(Files.newOutputStream(pkgFile), format);
        // remove base
        empty();
        return pkgFile;
    }

    private void deflate(OutputStream out, String format) throws IOException {
        switch(format) {
            case "zip":
            case "zip.nt":
                try (ZipOutputStream zout = new ZipOutputStream(
                                            new BufferedOutputStream(out))) {
                    fillZip(base, base.getFileName().toString(), zout, format.endsWith(".nt"));
                }
                break;
            case "tgz":
            case "tgz.nt":
                try (TarArchiveOutputStream tout = new TarArchiveOutputStream(
                                                   new BufferedOutputStream(
                                                   new GzipCompressorOutputStream(out)))) {
                    fillArchive(base, base.getFileName().toString(), tout, format.endsWith(".nt"));
                }
                break;
            default:
                throw new IOException("Unsupported package format: " + format);
        }
    }

    private void fillArchive(Path dirFile, String relBase, ArchiveOutputStream out, boolean skipTime) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirFile)) {
            for (Path file : stream) {
                String relPath = relBase + '/' + file.getFileName().toString();
                if (Files.isDirectory(file)) {
                    fillArchive(file, relPath, out, skipTime);
                } else {
                    TarArchiveEntry entry = new TarArchiveEntry(relPath);
                    entry.setSize(Files.size(file));
                    entry.setModTime(skipTime ? 0L : Files.getLastModifiedTime(file).toMillis());
                    out.putArchiveEntry(entry);
                    Files.copy(file, out);
                    out.closeArchiveEntry();
                }
            }
        }
    }

    private void fillZip(Path dirFile, String relBase, ZipOutputStream zout, boolean skipTime) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirFile)) {
            for (Path file : stream) {
                String relPath = relBase + '/' + file.getFileName().toString();
                if (Files.isDirectory(file)) {
                    fillZip(file, relPath, zout, skipTime);
                } else {
                    ZipEntry entry = new ZipEntry(relPath);
                    BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
                    entry.setCreationTime(skipTime ? FileTime.fromMillis(0L) : attrs.creationTime());
                    entry.setLastModifiedTime(skipTime ? FileTime.fromMillis(0L) : attrs.lastModifiedTime());
                    zout.putNextEntry(entry);
                    Files.copy(file, zout);
                    zout.closeEntry();
                }
            }
        }
    }
}
