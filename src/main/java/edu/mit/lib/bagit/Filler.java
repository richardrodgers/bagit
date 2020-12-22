/**
 * Copyright 2013, 2014 MIT Libraries
 * SPDX-Licence-Identifier: Apache-2.0
 */

package edu.mit.lib.bagit;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.tools.DiagnosticCollector;

import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import static edu.mit.lib.bagit.Bag.*;
import static edu.mit.lib.bagit.Bag.MetadataName.*;
import static edu.mit.lib.bagit.Filler.EolRule.*;

/**
 * Filler is a builder class used to construct bags conformant to IETF Bagit spec - version 1.0.
 * Filler objects serialize themselves to either a loose directory (degnerate case),
 * a compressed archive file (supported formats zip or tgz) or a stream of the archive,
 * abiding by the serialization recommendations of the specification.
 *
 * See README for sample invocations and API description.
 *
 * @author richardrodgers
 */

public class Filler {

    // directory root of bag
    private final Path base;
    // checksum algorithms
    private final Set<String> csAlgs;
    // Charset encoding for tag files
    private final Charset tagEncoding;
    // line separator used by FlatWriters
    private final String lineSeparator;
    // transient bag?
    private final boolean transientBag;
    // manifest writers
    private final Map<String, FlatWriter> tagWriters = new HashMap<>();
    private final Map<String, FlatWriter> manWriters = new HashMap<>();
    // optional flat writers
    private final Map<String, FlatWriter> writers = new HashMap<>();
    // optional bag stream
    private final Map<String, BagOutputStream> streams = new HashMap<>();
    // automatic metadata generation set
    private Set<MetadataName> autogenNames = Set.of(BAGGING_DATE, BAG_SIZE, PAYLOAD_OXUM, BAG_SOFTWARE_AGENT);
    // total payload size
    private long payloadSize = 0L;
    // number of payload files
    private int payloadCount = 0;
    // has bag been built?
    private boolean built;

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
     * a temporary directory to hold a transient bag with
     * default tag encoding (UTF-8), system-defined line separator,
     * and default checksum algorithm (SHA-512)
     *
     * @throws IOException if error creating bag
     */
    public Filler() throws IOException {
        this(Files.createTempDirectory("bag"), StandardCharsets.UTF_8, SYSTEM, true, DEFAULT_CS_ALGO);
    }

    /**
     * Returns a new Filler (bag builder) instance using passed
     * directory to hold a non-transient bag with
     * default tag encoding (UTF-8), system-defined line separator
     * and default checksum algorithm (SHA-512)
     *
     * @param base the base directory in which to construct the bag
     * @throws IOException if error creating bag
     */
    public Filler(Path base) throws IOException {
        this(base, StandardCharsets.UTF_8, SYSTEM, false, DEFAULT_CS_ALGO);
    }

    /**
     * Returns a new filler (bag builder) instance using passed
     * directory to hold a non-transient bag with
     * default tag encoding (UTF-8), system-defined line separator
     * and passed list of checksum algorithms (may be one)
     *
     * @param base the base directory in which to construct the bag
     * @param csAlgorithms list of checksum algorithms (may be one)
     * @throws IOException if error creating bag
     */
    public Filler(Path base, String ... csAlgorithms) throws IOException {
        this(base, StandardCharsets.UTF_8, SYSTEM, false, csAlgorithms);
    }

    /**
     * Returns a new filler (bag builder) instance using passed
     * directory to hold a non-transient bag with
     * passed tag encoding, system-defined line separator
     * and passed list of checksum algorithms (may be one)
     *
     * @param base the base directory in which to construct the bag
     * @param encoding tag encoding (currently UTF-8, UTF-16)
     * @param csAlgorithms list of checksum algorithms (may be one)
     * @throws IOException if error creating bag
     */
    public Filler(Path base, Charset encoding, String ... csAlgorithms) throws IOException {
        this(base, encoding, SYSTEM, false, csAlgorithms);
    }

    /**
     * Returns a new filler (bag builder) instances using passed directory,
     * tag encoding, and line separator for text files, transience rule,
     * and checksum algorithms
     *
     * @param base the base directory in which to construct the bag
     * @param encoding character encoding to use for tag files
     * @param eolRule line termination rule to use for generated text files. Values are:
     *            SYSTEM - use system-defined line termination
     *            COUNTER_SYSTEM - if on Windows, use Unix EOL, else reverse
     *            UNIX - use newline character line termination
     *            WINDOWS - use CR/LF line termination
     * @param transientBag if true, remove after reading network stream closes
     * @param csAlgorithms list of checksum algorithms (may be one)
     *
     * @throws IOException if error creating bag
     */
    public Filler(Path base, Charset encoding, EolRule eolRule, boolean transientBag, String ... csAlgorithms) throws IOException {
        this.base = base;
        tagEncoding = encoding;
        this.transientBag = transientBag;
        csAlgs = Set.of(csAlgorithms);
        Path dirPath = bagFile(DATA_DIR);
        if (Files.notExists(dirPath)) {
            Files.createDirectories(dirPath);
        }
        // verify these are legit and fail if not
        if (csAlgs.isEmpty()) {
            throw new IOException("No checksum algorithm specified");
        }
        for (String alg : csAlgs) {
            try {
                MessageDigest.getInstance(alg);
            } catch (NoSuchAlgorithmException nsaE) {
                throw new IOException("No such checksum algorithm: " + alg);
            }
            addWriters(alg);
        }
        String sysEol = System.lineSeparator();
        switch (eolRule) {
            case SYSTEM: lineSeparator = sysEol; break;
            case UNIX: lineSeparator = "\n"; break;
            case WINDOWS: lineSeparator = "\r\n"; break;
            case COUNTER_SYSTEM: lineSeparator = "\n".equals(sysEol) ? "\r\n" : "\n"; break;
            default: lineSeparator = sysEol; break;
        }
    }

    private void addWriters(String csAlgorithm) throws IOException {
        var sfx = csAlgoName(csAlgorithm) + ".txt";
        var tagWriter = new FlatWriter(bagFile(TAGMANIF_FILE + sfx), null, null, false, tagEncoding);
        tagWriters.put(csAlgorithm, tagWriter);
        manWriters.put(csAlgorithm, new FlatWriter(bagFile(MANIF_FILE + sfx), null, tagWriters, true, tagEncoding));
    }

    private void buildBag() throws IOException {
        if (built) return;
        // if auto-generating any metadata, do so
        for (MetadataName autogenName : autogenNames) {
            switch (autogenName) {
                case BAGGING_DATE:
                    metadata(BAGGING_DATE, new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
                    break;
                case BAG_SIZE:
                    metadata(BAG_SIZE, scaledSize(payloadSize, 0));
                    break;
                case PAYLOAD_OXUM:
                    metadata(PAYLOAD_OXUM, String.valueOf(payloadSize) + "." + String.valueOf(payloadCount));
                    break;
                case BAG_SOFTWARE_AGENT:
                    metadata(BAG_SOFTWARE_AGENT, "BagIt Lib v:" + LIB_VSN);
                    break;
                default:
                    break;
            }
        }
        // close all optional writers' tag files
        for (FlatWriter fw : writers.values()) {
            fw.close();
        }
        // close all optional output streams
        for (BagOutputStream bout : streams.values()) {
            bout.close();
        }
        // close the manifest files
        for (FlatWriter mw : manWriters.values()) {
            mw.close();
        }
        // write out bagit declaration file
        FlatWriter fwriter = new FlatWriter(bagFile(DECL_FILE), null, tagWriters, false, StandardCharsets.UTF_8);
        fwriter.writeLine("BagIt-Version: " + BAGIT_VSN);
        fwriter.writeLine("Tag-File-Character-Encoding: " + tagEncoding.name());
        fwriter.close();
        // close tag manifest files of previous tag files
        for (FlatWriter tw : tagWriters.values()) {
            tw.close();
        }
        built = true;
    }

    /**
     * Assigns the set of automatically generated metadata identifed by their names,
     * replacing default set: Bagging-Date, Bag-Size, Payload-Oxnum, Bag-Software-Agent
     * To disable automatic generation, pass in an empty set.
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
        commitPayload(relPath, Files.newInputStream(file));
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
        commitPayload(relPath, is);
        return this;
    }

    private void commitPayload(String relPath, InputStream is) throws IOException {
        Path payloadFile = dataFile(relPath);
        if (Files.exists(payloadFile)) {
            throw new IllegalStateException("Payload file already exists at: " + relPath);
        }
        payloadSize += digestCopy(is, payloadFile, DATA_PATH + relPath, manWriters);
        payloadCount++;
    }

    private long digestCopy(InputStream is, Path destFile, String relPath, Map<String, FlatWriter> fwriters) throws IOException {
         // wrap stream in digest streams
         var digests = new HashMap<String, MessageDigest>();
         var iter = csAlgs.iterator();
         long copied = 0L;
         try {
             InputStream dis = is;
             while (iter.hasNext()) {
                 var alg = iter.next();
                 var dg = MessageDigest.getInstance(csAlgoCode(alg));
                 digests.put(alg, dg);
                 dis = new DigestInputStream(dis, dg);
             }
             copied = Files.copy(dis, destFile);
             // record checksums
             for (String alg : csAlgs) {
                 fwriters.get(alg).writeLine(toHex(digests.get(alg).digest()) + " " + relPath);
             }
         } catch (NoSuchAlgorithmException nsaE) {
             // should never occur, algorithms checked in constructor
             throw new IOException("bad algorithm");
         }
         return copied;
    }

    /**
     * Obtains manifest of bag contents.
     *
     * @param csAlgorithm the checksum used by manifest
     * @return List manifest list
     */
    public List<String> getManifest(String csAlgorithm) {
        return List.copyOf(manWriters.get(csAlgorithm).getLines());
    }

    /**
     * Adds a resource identified by a URI reference to payload contents 
     * i.e. to the fetch.txt file.
     *
     * @param relPath the bag-relative path of the resource
     * @param file the file to add to fetch list
     * @param uri the URI of the resource
     * @return Filler this Filler
     * @throws IOException if error reading/writing ref data
     */
    public Filler payloadRef(String relPath, Path file, URI uri) throws IOException {
        return payloadRef(relPath, Files.newInputStream(file), uri);
    }

    /**
     * Adds a resource identified by a URI reference to payload contents 
     * i.e. to the fetch.txt file.
     *
     * @param relPath the bag-relative path of the resource
     * @param in the input stream of resource to add to fetch list
     * @param uri the URI of the resource
     * @return Filler this Filler
     * @throws IOException if error reading/writing ref data
     */
    public Filler payloadRef(String relPath, InputStream in, URI uri) throws IOException {
        Path payloadFile = dataFile(relPath);
        if (Files.exists(payloadFile)) {
            throw new IllegalStateException("Payload file already exists at: " + relPath);
        }
        if (! uri.isAbsolute()) {
            throw new IOException("URI must be absolute");
        }
        FlatWriter refWriter = getWriter(REF_FILE);
        var destDir = Files.createTempDirectory("null");
        var destFile = destDir.resolve("foo");
        long size = digestCopy(in, destFile, relPath, manWriters);
        var sizeStr = (size > 0L) ? Long.toString(size) : "-";
        refWriter.writeLine(uri.toString() + " " + sizeStr + " " + DATA_PATH + relPath);
        Files.delete(destFile);
        Files.delete(destDir);
        return this;
    }

    /**
     * Adds a resource identified by a URI reference to payload contents 
     * i.e. to the fetch.txt file. Caller assumes full responsibility for
     * ensuring correctness of checksums and size - library does not verify.
     *
     * @param relPath the bag-relative path of the resource
     * @param size the expected size of the resource in bytes
     * @param uri the URI of the resource
     * @param checksums map of algorithms to checksums of the resource
     * @return Filler this Filler
     * @throws IOException if error reading/writing ref data
     */
    public Filler payloadRefUnsafe(String relPath, long size, URI uri, Map<String, String> checksums) throws IOException {
        Path payloadFile = dataFile(relPath);
        if (Files.exists(payloadFile)) {
            throw new IllegalStateException("Payload file already exists at: " + relPath);
        }
        if (! uri.isAbsolute()) {
            throw new IOException("URI must be absolute");
        }
        if (! checksums.keySet().equals(csAlgs)) {
            throw new IOException("checksums do not match bags");
        }
        for (String alg : manWriters.keySet()) {
            manWriters.get(alg).writeLine(checksums.get(alg) + " " + relPath);
        }
        var sizeStr = (size > 0L) ? Long.toString(size) : "-";
        FlatWriter refWriter = getWriter(REF_FILE);
        refWriter.writeLine(uri.toString() + " " + sizeStr + " " + DATA_PATH + relPath);
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
        return getStream(dataFile(relPath), relPath, true);
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
        digestCopy(is, tagFile(relPath), relPath, tagWriters);
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
        return getStream(tagFile(relPath), relPath, false);
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
            writer = new FlatWriter(bagFile(name), null, tagWriters, false, tagEncoding);
            writers.put(name, writer);
        }
        return writer;
    }

    private BagOutputStream getStream(Path path, String name, boolean isPayload) throws IOException {
        BagOutputStream stream = streams.get(name);
        if (stream == null) {
            var relPath = isPayload ? DATA_PATH + name : name;
            var writers = isPayload ? manWriters : tagWriters;
            stream = new BagOutputStream(path, relPath, writers);
            streams.put(name, stream);
        }
        return stream;
    }

    class FlatWriter extends BagOutputStream {

        private final List<String> lines = new ArrayList<>();
        private final boolean record;
        private final Charset encoding;
        private final AtomicBoolean bomOut = new AtomicBoolean();

        private FlatWriter(Path file, String brPath, Map<String, FlatWriter> tailWriters, boolean record, Charset encoding) throws IOException {
            super(file, brPath, tailWriters);
            this.record = record;
            this.encoding = encoding;
        }

        public void writeProperty(String key, String value) throws IOException {
            String prop = key + ": " + value;
            int offset = 0;
            while (offset < prop.length()) {
                int end = Math.min(prop.length() - offset, 80);
                if (offset > 0) {
                    write(filterBytes(SPACER, encoding, bomOut));
                }
                writeLine(prop.substring(offset, offset + end));
                offset += end;
            }
        }

        public void writeLine(String line) throws IOException {
            if (record) {
                lines.add(line);
            }
            write(filterBytes(line + lineSeparator, encoding, bomOut));
        }

        public List<String> getLines() {
            return lines;
        }
    }

    // wraps output stream in digester, and records results with tail writer
    class BagOutputStream extends OutputStream {

        private final String relPath;
        private final Map<String, FlatWriter> tailWriters;
        private OutputStream out;
        private HashMap<String, MessageDigest> digests;
        private boolean closed = false;

        private BagOutputStream(Path file, String relPath, Map<String, FlatWriter> tailWriters) throws IOException {
            this.relPath = (relPath != null) ? relPath : file.getFileName().toString();
            this.tailWriters = tailWriters;
            out = Files.newOutputStream(file);
            if (tailWriters != null) {
                // wrap stream in digest streams
                digests = new HashMap<String, MessageDigest>();
                var iter = csAlgs.iterator();
                try {
                    while (iter.hasNext()) {
                        var alg = iter.next();
                        var dg = MessageDigest.getInstance(csAlgoCode(alg));
                        digests.put(alg, dg);
                        out = new DigestOutputStream(out, dg);
                    }
                } catch (NoSuchAlgorithmException nsae) {
                    // should never occur - algorithms checked in constructor
                    throw new IOException("no such algorithm");
                }
            }
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
        }

        @Override
        public synchronized void close() throws IOException {
            if (! closed) {
                out.flush();
                if (tailWriters != null) {
                    // record checksums
                    for (String alg : csAlgs) {
                        tailWriters.get(alg).writeLine(toHex(digests.get(alg).digest()) + " " + relPath);
                    }
                }
                out.close();
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
     * Returns bag serialized as an archive file using default packaging (zip archive)
     * assigning zip timestamps
     *
     * @return path the bag archive package path
     * @throws IOException if error reading bag
     */
    public Path toPackage() throws IOException {
        return toPackage(DFLT_FMT, false);
    }

    /**
     * Returns bag serialized as an archive file using passed packaging format.
     * Supported formats: 'zip' - zip archive, 'tgz' - gzip compressed tar archive
     *
     * @param format the package format ('zip', or 'tgz')
     * @param noTime if 'true', suppress regular timestamp assignment in archive
     * @return path the bag archive package path
     * @throws IOException if error reading bag
     */
    public Path toPackage(String format, boolean noTime) throws IOException {
        return deflate(format, noTime);
    }

    /**
     * Returns bag serialized as an IO stream using default packaging (zip archive).
     * Bag is deleted when stream closed if temporary bag location used.
     *
     * @return InputStream of the bag archive package
     * @throws IOException if error reading bag
     */
    public InputStream toStream() throws IOException {
        return toStream(DFLT_FMT, true);
    }

    /**
     * Returns bag serialized as an IO stream using passed packaging format.
     * Bag is deleted when stream closed if temporary bag location used.
     * Supported formats: 'zip' - zip archive, 'tgz' - gzip compressed tar archive
     *
     * @param format the package format ('zip', or 'tgz')
     * @param noTime if 'true', suppress regular timestamp assignment in archive
     * @return InputStream of the bag archive package
     * @throws IOException if error reading bag
     */
    public InputStream toStream(String format, boolean noTime) throws IOException {
        Path pkgFile = deflate(format, noTime);
        if (transientBag) {
            return new CleanupInputStream(Files.newInputStream(pkgFile), pkgFile);
        } else {
            return Files.newInputStream(pkgFile);
        }
    }

    class CleanupInputStream extends FilterInputStream {

        private final Path file;

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

    private Path deflate(String format, boolean noTime) throws IOException {
        // deflate this bag in situ (in current directory) using given packaging format
        buildBag();
        Path pkgFile = base.getParent().resolve(base.getFileName().toString() + "." + format);
        deflate(Files.newOutputStream(pkgFile), format, noTime);
        // remove base
        empty();
        return pkgFile;
    }

    private void deflate(OutputStream out, String format, boolean noTime) throws IOException {
        switch(format) {
            case "zip":
                try (ZipOutputStream zout = new ZipOutputStream(
                                            new BufferedOutputStream(out))) {
                    fillZip(base, base.getFileName().toString(), zout, noTime);
                }
                break;
            case "tgz":
                try (TarArchiveOutputStream tout = new TarArchiveOutputStream(
                                                   new BufferedOutputStream(
                                                   new GzipCompressorOutputStream(out)))) {
                    fillArchive(base, base.getFileName().toString(), tout, noTime);
                }
                break;
            default:
                throw new IOException("Unsupported package format: " + format);
        }
    }

    private void fillArchive(Path dirFile, String relBase, ArchiveOutputStream out, boolean noTime) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirFile)) {
            for (Path file : stream) {
                String relPath = relBase + '/' + file.getFileName().toString();
                if (Files.isDirectory(file)) {
                    fillArchive(file, relPath, out, noTime);
                } else {
                    TarArchiveEntry entry = new TarArchiveEntry(relPath);
                    entry.setSize(Files.size(file));
                    entry.setModTime(noTime ? 0L : Files.getLastModifiedTime(file).toMillis());
                    out.putArchiveEntry(entry);
                    Files.copy(file, out);
                    out.closeArchiveEntry();
                }
            }
        }
    }

    private void fillZip(Path dirFile, String relBase, ZipOutputStream zout, boolean noTime) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirFile)) {
            for (Path file : stream) {
                String relPath = relBase + '/' + file.getFileName().toString();
                if (Files.isDirectory(file)) {
                    fillZip(file, relPath, zout, noTime);
                } else {
                    ZipEntry entry = new ZipEntry(relPath);
                    BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
                    entry.setCreationTime(noTime ? FileTime.fromMillis(0L) : attrs.creationTime());
                    entry.setLastModifiedTime(noTime ? FileTime.fromMillis(0L) : attrs.lastModifiedTime());
                    zout.putNextEntry(entry);
                    Files.copy(file, zout);
                    zout.closeEntry();
                }
            }
        }
    }
}
