/**
 * Copyright 2013, 2014 MIT Libraries
 * SPDX-Licence-Identifier: Apache-2.0
 */

package edu.mit.lib.bagit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.mit.lib.bagit.Filler.EolRule;

/**
 * Bag represents a rudimentary bag conformant to IETF Bagit spec: rfc 8493 version 1.0.
 * A Bag is a directory with contents and a little structured metadata in it.
 * Although they don't have to be, these bags are 'wormy' - meaning that they
 * can be written only once (have no update semantics), and can be 'holey' -
 * meaning they can contain content by reference as well as inclusion.
 *
 * Bags are not directly instantiated - package helper classes (Filler, Loader)
 * can create bags and serialize them to a compressed archive file (supported
 * formats zip or tgz) or be deserialized from same or a stream,
 * abiding by the serialization recommendations of the specification.
 *
 * See README for sample invocations and API description.
 *
 * @author richardrodgers
 */

public class Bag {
    // coding constants
    static final String DEFAULT_CS_ALGO = "SHA-512";
    static final String BAGIT_VSN = "1.0";
    static final String LIB_VSN = "1.0";
    static final String DFLT_FMT = "zip";
    static final String TGZIP_FMT = "tgz";
    static final String SPACER = " ";
    // required file and directory names
    static final String MANIF_FILE = "manifest-";
    static final String TAGMANIF_FILE = "tagmanifest-";
    static final String DECL_FILE = "bagit.txt";
    static final String META_FILE = "bag-info.txt";
    static final String REF_FILE = "fetch.txt";
    static final String DATA_DIR = "data";
    static final String DATA_PATH = DATA_DIR + "/";

    /**
     * Reserved metadata property names
     */
    public enum MetadataName {
        SOURCE_ORG("Source-Organization"),
        ORG_ADDR("Organization-Address"),
        CONTACT_NAME("Contact-Name"),
        CONTACT_PHONE("Contact-Phone"),
        CONTACT_EMAIL("Contact-Email"),
        EXTERNAL_DESC("External-Description"),
        EXTERNAL_ID("External-Identifier"),
        BAGGING_DATE("Bagging-Date"),
        BAG_SIZE("Bag-Size"),
        PAYLOAD_OXUM("Payload-Oxum"),
        BAG_GROUP_ID("Bag-Group-Identifier"),
        BAG_COUNT("Bag-Count"),
        INTERNAL_SENDER_ID("Internal-Sender-Identifier"),
        INTERNAL_SENDER_DESC("Internal-Sender-Description"),
        BAG_SOFTWARE_AGENT("Bag-Software-Agent");  // not in IETF spec

        private final String mdName;

        MetadataName(String name) {
            mdName = name;
        }

        public String getName() {
            return mdName;
        }
    }

    // directory root of bag
    private final Path baseDir;

    // Character encoding declared for tag files
    private final Charset tagEncoding;

    // allow serialization, etc of bag
    private final boolean sealed;

    // metadata cache
    private final Map<String, Map<String, List<String>>> mdCache = new HashMap<>();

    /**
     * Constructor - creates a new bag from a Loader
     *
     */
    Bag(Path baseDir, boolean sealed) throws IOException {
        this.baseDir = baseDir;
        this.sealed = sealed;
        this.tagEncoding = tagEncoding(baseDir);
    }

    /**
     * Returns the specification version of the bag.
     *
     * @return version the BagIt spec version string
     */
    public static String bagItVersion() {
        return BAGIT_VSN;
    }

    /**
     * Returns the software version of the library.
     *
     * @return version the software version string
     */
    public static String libVersion() {
        return LIB_VSN;
    }

    /**
     * Returns the name of the bag.
     *
     * @return name the name of the bag
     */
    public String bagName() {
        return baseDir.getFileName().toString();
    }

    /**
     * Returns the checksum algortihms used in bag manifests.
     *
     * @return algorithms the set of checksum algorithms
     * @throws IOException if algorithms unknown
     */
    public Set<String> csAlgorithms() throws IOException {
        return csAlgorithms(baseDir);
    }

    /**
     * Returns the character encoding declared for tag files.
     *
     * @return declared character set encoding
     * @throws IOException if encoding missing or unknown
     */
    public Charset tagEncoding() throws IOException {
        return tagEncoding;
    }

    /**
     * Returns the line separator used by tag files
     * 
     * @return EOLRule separator the line separator
     * @throws IOException if unable to read tag file
     */
    public EolRule lineSeparator() throws IOException {
        try (InputStream in = Files.newInputStream(bagFile(DECL_FILE))) {
            byte[] buf = in.readNBytes(20);
            switch(buf[18]) {
                case '\n': return EolRule.UNIX;
                case '\r': return (buf[19] == '\n') ? EolRule.WINDOWS : EolRule.CLASSIC;
                default: return EolRule.UNIX;
            }
        }
    }

    /**
     * Returns whether the bag is complete.
     *
     * @return complete true if bag is complete
     * @throws IOException if unable to read bag contents
     */
    public boolean isComplete() throws IOException {
        return completeStatus() == 0;
    }

    /**
     * Returns completeness status code.
     *
     * @return status code for completeness: 0 is complete, negative is error code
     * @throws IOException if unable to read bag contents
     */
    public int completeStatus() throws IOException {
        // no fetch.txt?
        if (Files.exists(bagFile(REF_FILE))) return -1;
        // mandatory files present and correct?
        if (! Files.exists(bagFile(DECL_FILE))) return -2;
        if (! Charset.isSupported(tagEncoding.name())) return -3;
        if (! Files.isDirectory(bagFile(DATA_DIR))) return -4;
        // payload files and tag files map?
        for (String csAlg: csAlgorithms()) {
            var code = compareManifest(csAlg);
            if (code != 0)
                return code;
            code = compareTag(csAlg);
            if (code != 0)
                return code;
        }
        return 0;
    }

    private int compareManifest(String csAlgorithm) throws IOException {
        Map<String, String> payloads = payloadManifest(csAlgorithm);
        // # payload files and # manifest entries must agree
        if (fileCount(bagFile(DATA_DIR)) != payloads.size()) return -5;
        // files themselves must match also
        for (String path : payloads.keySet()) {
            if (path.startsWith(DATA_DIR) && Files.notExists(bagFile(path))) return -6;
        }
        return 0;
    }

    private int compareTag(String csAlgorithm) throws IOException {
        Map<String, String> tags = tagManifest(csAlgorithm);
        // # tag files and # manifest entries must agree
        // tag files consist of any top-level files except:
        // tagmanifest itself, and the payload directory.
        DirectoryStream.Filter<Path> filter = file -> {
            String name = file.getFileName().toString();
            return ! (name.startsWith(TAGMANIF_FILE) || name.startsWith(DATA_DIR));
        };
        int tagCount = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(baseDir, filter)) {
            for (Path tag : stream) {
                if (Files.isDirectory(tag)) tagCount += fileCount(tag);
                else tagCount++;
            }
        }
        if (tagCount != tags.size()) return -7;
        // files themselves must match also
        for (String path : tags.keySet()) {
            if (Files.notExists(bagFile(path))) return -8;
        }
        return 0;
    }

    /**
     * Returns whether the bag is sealed.
     *
     * @return sealed true if bag is sealed
     */
    public boolean isSealed() {
        return sealed;
    }

    /**
     * Returns whether the bag is valid.
     *
     * @return valid true if bag validates
     * @throws IOException if unable to read bag contents
     */
    public boolean isValid() throws IOException {
        return validationStatus() == 0;
    }

    /**
     * Returns validation status code
     *
     * @return 0 if bag validates, else a negative number
     * @throws IOException if unable to read bag contents
     */
    public int validationStatus() throws IOException {
        int status = completeStatus();
        if (status != 0) return status;
        // recompute all checksums and compare against manifest values
        for (String csAlg : csAlgorithms()) {
            var code = recomputeChecksum(csAlg);
            if (code != 0)
                return code;
        }
        return 0;
    }

    private int recomputeChecksum(String csAlgorithm) throws IOException {
        Map<String, String> payloads = payloadManifest(csAlgorithm);
        for (String relPath : payloads.keySet()) {
            int offset = relPath.startsWith("./") ? 2 : 0;
            String cutPath = relPath.substring(DATA_PATH.length() + offset);
            if (! validateFile(payloadStream(cutPath), payloads.get(relPath), csAlgorithm)) return -9;
        }
        // same for tag files
        Map<String, String> tags = tagManifest(csAlgorithm);
        for (String relPath : tags.keySet()) {
            if (! validateFile(tagStream(relPath), tags.get(relPath), csAlgorithm)) return -10;
        }
        return 0;
    }

    /**
     * Returns the payload file for the passed relative path name
     *
     * @param relPath the relative path of the file from the data root directory
     * @return the payload file, or null if no file at the specified path
     * @throws IOException if unaable to access bag
     * @throws IllegalAccessException if bag is sealed
     */
    public Path payloadFile(String relPath) throws IOException, IllegalAccessException {
        if (sealed) {
            throw new IllegalAccessException("Sealed Bag: no file access allowed");
        }
        Path payload = dataFile(relPath);
        return Files.exists(payload) ? payload : null;
    }

    /**
     * Returns the payload file attributes of the passed relative path name.
     * Useful for obtaining file info from sealed Bags
     *
     * @param relPath the relative path of the file from the data root directory
     * @return the payload file attributes, or null if no file at the specified path
     * @throws IOException if unable to read file attributes
     */
    public BasicFileAttributes payloadFileAttributes(String relPath) throws IOException {
        Path payload = dataFile(relPath);
        return Files.exists(payload) ? Files.readAttributes(payload, BasicFileAttributes.class) : null;
    }

    /**
     * Returns an input stream for the passed payload relative path name
     *
     * @param relPath the relative path of the file from the data root directory
     * @return in InputStream, or null if no file at the specified path
     * @throws IOException if unable to open input stream
     */
    public InputStream payloadStream(String relPath) throws IOException {
        Path payload = dataFile(relPath);
        return Files.exists(payload) ? Files.newInputStream(payload) : null;
    }

    /**
     * Returns a map of payload files to their fetch URLs and
     * estimated sizes (i.e. contents of fetch.txt). Map keys
     * are bag-relative file paths, and values have the form:
     * "size url" where 'size' is numeric or '-' and a space
     * separates the size from the URL.
     *
     * @return refMap the map of payload files to fetch URLs
     * @throws IOException if unable to read refs data
     */
    public Map<String, String> payloadRefs() throws IOException {
        return payloadRefs(bagFile(REF_FILE), tagEncoding);
    }

    /**
     * Returns a tag file for the passed tag relative path name
     *
     * @param relPath the relative path of the file from the bag root directory
     * @return tagfile the tag file path, or null if no file at the specified path
     * @throws IllegalAccessException if bag is sealed
     */
    public Path tagFile(String relPath) throws IllegalAccessException {
        if (sealed) {
            throw new IllegalAccessException("Sealed Bag: no file access allowed");
        }
        Path tagFile = bagFile(relPath);
        return Files.exists(tagFile) ? tagFile : null;
    }

    /**
     * Returns the tag file attributes of the passed relative path name.
     * Useful for obtaining file info from sealed Bags
     *
     * @param relPath the relative path of the file from the bag root directory
     * @return the tag file attributes, or null if no file at the specified path
     * @throws IOException if unable to access tag file
     */
    public BasicFileAttributes tagFileAttributes(String relPath) throws IOException {
        Path tagFile = bagFile(relPath);
        return Files.exists(tagFile) ? Files.readAttributes(tagFile, BasicFileAttributes.class) : null;
    }

    /**
     * Returns an input stream for the passed tag relative path name
     *
     * @param relPath the relative path of the file from the bag root directory
     * @return in InputStream, or null if no file at the specified path
     * @throws IOException if unable to open tag file stream
     */
    public InputStream tagStream(String relPath) throws IOException {
        Path tagFile = bagFile(relPath);
        return Files.exists(tagFile) ? Files.newInputStream(tagFile) : null;
    }

     /**
     * Returns the names of all the metadata properties in
     * the standard metadata file (bag-info.txt) in order declared.
     *
     * @return values property values for passed name, or empty list if no such property defined.
     * @throws IOException if unable to read metadata
     */
    public List<String> metadataNames() throws IOException {
        var names = new ArrayList<String>();
        try (BufferedReader reader = Files.newBufferedReader(bagFile(META_FILE), tagEncoding)) {
            String line;
            while ((line = reader.readLine()) != null) {
                // if line does not start with spacer, it is a new property
                if (! line.startsWith(SPACER)) {
                    // add name if new
                    int split = line.indexOf(":");
                    var propName = line.substring(0, split).trim();
                    if (! names.contains(propName)) {
                        names.add(propName);
                    }
                } 
            }
        }
        return names;
    }

    /**
     * Returns the values, if any, of a reserved metadata property in
     * the standard metadata file (bag-info.txt) in order declared.
     *
     * @param mdName the metadata property name
     * @return values property values for passed name, or empty list if no such property defined.
     * @throws IOException if unable to read metadata
     */
    public List<String> metadata(MetadataName mdName) throws IOException {
        return metadata(mdName.getName());
    }

    /**
     * Returns the values, if any, of the named metadata property in
     * standard metadata file (bag-info.txt) in order declared.
     *
     * @param name the metadata property name
     * @return values property value for passed name, or empty list if no such property defined.
     * @throws IOException if unable to read metadata
     */
    public List<String> metadata(String name) throws IOException {
        return property(META_FILE, name);
    }

    /**
     * Returns the values of a property, if any, in named file in order declared.
     * Typically used for metadata properties in tag files.
     *
     * @param relPath bag-relative path to the property file
     * @param name the property name
     * @return values property value for passed name, or empty list if no such property defined.
     * @throws IOException if unable to read metadata
     */
    public List<String> property(String relPath, String name) throws IOException {
        Map<String, List<String>> mdSet = mdCache.get(relPath);
        if (mdSet == null) {
            synchronized (mdCache) {
                mdSet = new HashMap<>();
                try (BufferedReader reader = Files.newBufferedReader(bagFile(relPath), tagEncoding)) {
                    String propName = null;
                    StringBuilder valSb = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        // if line does not start with spacer, it is a new property
                        if (! line.startsWith(SPACER)) {
                            // write pendng data if present
                            if (propName != null) {
                                addProp(propName, valSb.toString(), mdSet);
                                valSb = new StringBuilder();
                            }
                            int split = line.indexOf(":");
                            propName = line.substring(0, split);
                            valSb.append(line.substring(split + 1).trim());
                        } else {
                            valSb.append(line.substring(SPACER.length()));
                        }
                    }
                    addProp(propName, valSb.toString(), mdSet);
                    mdCache.put(relPath, mdSet);
                }
            }
        }
        return mdSet.get(name);
    }

    /**
     * Returns the contents of the payload manifest file.
     * Contents are relative paths as keys and checksums as values.
     *
     * @param csAlgorithm the checksum algorithm of manifest file
     * @return map a map of resource path names to checksums
     * @throws IOException if unable to read manifest
     */
    public Map<String, String> payloadManifest(String csAlgorithm) throws IOException {
        return manifest(MANIF_FILE + csAlgoName(csAlgorithm) + ".txt");
    }

    /**
     * Returns the contents of the tag manifest file.
     * Contents are relative paths as keys and checksums as values.
     * 
     * @param csAlgorithm the checksum algorithm of tag manifest file
     * @return map a map of resource path names to checksums
     * @throws IOException if unable to read tag manifest
     */
    public Map<String, String> tagManifest(String csAlgorithm) throws IOException {
        return manifest(TAGMANIF_FILE + csAlgoName(csAlgorithm) + ".txt");
    }

    /**
     * Returns the contents of manifest file at relative path name.
     * Contents are relative paths as keys and checksums as values.
     * An empty map is returned if no manifest at path.
     *
     * @param relPath the package-relative path to the manifest file
     * @return map a map of resource path names to checksums
     * @throws IOException if unable to read manifest
     */
    public Map<String, String> manifest(String relPath) throws IOException {
        Map<String, String> mfMap = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(bagFile(relPath), tagEncoding)) {
            String line;
            while((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+", 2);
                mfMap.put(parts[1], parts[0]);
            }
        }
        return mfMap;
    }

    // count of files in a directory, including subdirectory files (but not the subdir itself)
    private int fileCount(Path dir) throws IOException {
        int count = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path file : stream) {
                if (Files.isDirectory(file)) {
                    count += fileCount(file);
                } else {
                    count++;
                }
            }
        }
        return count;
    }

    private void addProp(String name, String value, Map<String, List<String>> mdSet) {
        List<String> vals = mdSet.computeIfAbsent(name, k -> new ArrayList<>());
        vals.add(value.trim());
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

    private Path bagFile(String name) {
        return baseDir.resolve(name);
    }

    private static boolean validateFile(InputStream is, String expectedChecksum, String csAlg) throws IOException {
        if (is == null) {
            throw new IOException("no input");
        }
        byte[] buf = new byte[2048];
        int num = 0;
        // wrap stream in digest stream
        try (DigestInputStream dis =
            new DigestInputStream(is, MessageDigest.getInstance(csAlgoCode(csAlg)))) {
            while (num != -1) {
                num = dis.read(buf);
            }
            return (expectedChecksum.equals(toHex(dis.getMessageDigest().digest())));
        } catch (NoSuchAlgorithmException nsaE) {
            throw new IOException("no algorithm: " + csAlg);
        }
    }

    static Map<String, String> payloadRefs(Path refFile, Charset encoding) throws IOException {
        Map<String, String> refMap = new HashMap<>();
        if (Files.exists(refFile)) {
            try (BufferedReader reader = Files.newBufferedReader(refFile, encoding)) {
                String line;
                while((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    String value = parts[1] + SPACER + parts[0];
                    refMap.put(parts[2], value);
                }
            }
        }
        return refMap;
    }

    static Set<String> csAlgorithms(Path base) throws IOException {
        // determine checksums in use from the manifest file names
        var csAlgs = new HashSet<String>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(base, MANIF_FILE + "*")) {
            for (Path manFile : stream) {
                // extract algorithm from filename
                String fileName = manFile.getFileName().toString();
                csAlgs.add(csAlgoCode(fileName.substring(MANIF_FILE.length(), fileName.lastIndexOf("."))));
            }
        }
        return csAlgs;
    }

    static Charset tagEncoding(Path base) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(base.resolve(DECL_FILE), StandardCharsets.UTF_8)) {
            // second line has encoding
            reader.readLine();
            String line = reader.readLine();
            if (line != null) {
                String[] parts = line.split(":");
                return Charset.forName(parts[1].trim());
            }
        }
        return Charset.defaultCharset();
    }

    static String csAlgoName(String csAlgol) {
        return csAlgol.toLowerCase().replace("-","");
    }

    static String csAlgoCode(String csAlgol) {
        String csa = csAlgol.toUpperCase();
        if (csa.startsWith("SHA") && csa.indexOf("-") == -1) {
            return "SHA-" + csa.substring(3);
        } else {
            return csa;
        }
    }

    static byte[] filterBytes(String data, Charset encoding, AtomicBoolean bomOut) {
        byte[] dbytes = data.getBytes(encoding);
        if (bomOut.compareAndSet(false, true)) {
            return dbytes;
        } else if (encoding.equals(StandardCharsets.UTF_16) ||
                   encoding.equals(StandardCharsets.UTF_16BE) ||
                   encoding.equals(StandardCharsets.UTF_16LE)) {
            return Arrays.copyOfRange(dbytes, 2, dbytes.length);
        } else {
            return dbytes;
        }
    }

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
    static String toHex(byte[] data) {
        if ((data == null) || (data.length == 0)) {
            return null;
        }
        char[] chars = new char[2 * data.length];
        for (int i = 0; i < data.length; ++i) {
            chars[2 * i] = HEX_CHARS[(data[i] & 0xF0) >>> 4];
            chars[2 * i + 1] = HEX_CHARS[data[i] & 0x0F];
        }
        return new String(chars);
    }

    private static final String[] tags = {"bytes", "KB", "MB", "GB", "TB" };
    static String scaledSize(long size, int index) {
        if (size < 1000) {
            return size + " " + tags[index];
        } else {
            return scaledSize(size / 1000, index + 1);
        }
    }
}
