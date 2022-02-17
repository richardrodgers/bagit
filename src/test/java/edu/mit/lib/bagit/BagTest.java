/**
 * Copyright 2013, 2014 MIT Libraries
 * SPDX-Licence-Identifier: Apache-2.0
 */
package edu.mit.lib.bagit;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.temporal.ChronoUnit;
import java.util.Scanner;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.nio.file.StandardCopyOption.*;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

import static edu.mit.lib.bagit.Bag.*;
import static edu.mit.lib.bagit.Bag.MetadataName.*;
import static edu.mit.lib.bagit.Loader.*;

/*
 * Basic unit tests for BagIt Library. Incomplete.
 */

@RunWith(JUnit4.class)
public class BagTest {

    public Path payload1, payload2, tag1, tag2;

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void createTestData() throws IOException {
         payload1 = tempFolder.newFile("payload1").toPath();
         // copy some random bits
         OutputStream out = Files.newOutputStream(payload1);
         for (int i = 0; i < 1000; i++) {
             out.write("lskdflsfevmep".getBytes());
         }
         out.close();
         // copy to all other test files
         payload2 = tempFolder.newFile("payload2").toPath();
         Files.copy(payload1, payload2, REPLACE_EXISTING);
         tag1 = tempFolder.newFile("tag1").toPath();
         Files.copy(payload1, tag1, REPLACE_EXISTING);
         tag2 = tempFolder.newFile("tag2").toPath();
         Files.copy(payload1, tag2, REPLACE_EXISTING);
    }

    // tests for various checksum related functions

    @Test
    public void basicBagPartsPresentDefaultChecksum() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("cs-bag1").toPath();
        new Filler(bagFile).payload(payload1).toDirectory();
        Path decl = bagFile.resolve(DECL_FILE);
        assertTrue(Files.exists(decl));
        // should default to SHA-512
        Path manifest = bagFile.resolve("manifest-sha512.txt");
        assertTrue(Files.exists(manifest));
        Path tagmanifest = bagFile.resolve("tagmanifest-sha512.txt");
        assertTrue(Files.exists(tagmanifest));
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path payloadFile = payloadDir.resolve(payload1.getFileName().toString());
        assertTrue(Files.exists(payloadFile));
        // assure completeness
        Bag bag = load(bagFile);
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test
    public void basicBagPartsPresentMD5() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("cs-bag2").toPath();
        new Filler(bagFile, "MD5").payload(payload1).toDirectory();
        Path decl = bagFile.resolve(DECL_FILE);
        assertTrue(Files.exists(decl));
        Path manifest = bagFile.resolve("manifest-md5.txt");
        assertTrue(Files.exists(manifest));
        Path tagmanifest = bagFile.resolve("tagmanifest-md5.txt");
        assertTrue(Files.exists(tagmanifest));
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path payloadFile = payloadDir.resolve(payload1.getFileName().toString());
        assertTrue(Files.exists(payloadFile));
        // assure completeness
        Bag bag = load(bagFile);
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test
    public void basicBagPartsPresentSHA1() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("cs-bag3").toPath();
        new Filler(bagFile, "SHA-1").payload(payload1).toDirectory();
        Path decl = bagFile.resolve(DECL_FILE);
        assertTrue(Files.exists(decl));
        Path manifest = bagFile.resolve("manifest-sha1.txt");
        assertTrue(Files.exists(manifest));
        Path tagmanifest = bagFile.resolve("tagmanifest-sha1.txt");
        assertTrue(Files.exists(tagmanifest));
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path payloadFile = payloadDir.resolve(payload1.getFileName().toString());
        assertTrue(Files.exists(payloadFile));
        // assure completeness
        Bag bag = load(bagFile);
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test
    public void basicBagPartsPresentSHA256() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("cs-bag4").toPath();
        new Filler(bagFile, "SHA-256").payload(payload1).toDirectory();
        Path decl = bagFile.resolve(DECL_FILE);
        assertTrue(Files.exists(decl));
        Path manifest = bagFile.resolve("manifest-sha256.txt");
        assertTrue(Files.exists(manifest));
        Path tagmanifest = bagFile.resolve("tagmanifest-sha256.txt");
        assertTrue(Files.exists(tagmanifest));
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path payloadFile = payloadDir.resolve(payload1.getFileName().toString());
        assertTrue(Files.exists(payloadFile));
        // assure completeness
        Bag bag = load(bagFile);
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test(expected = IOException.class)
    public void unknownChecksumAlgorithm() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("cs-bag5").toPath();
        new Filler(bagFile, "SHA-6666").payload(payload1).toDirectory();
        Path decl = bagFile.resolve(DECL_FILE);
        assertTrue(Files.exists(decl));
        Path manifest = bagFile.resolve("manifest-sha6666.txt");
        assertTrue(Files.exists(manifest));
        Path tagmanifest = bagFile.resolve("tagmanifest-sha6666.txt");
        assertTrue(Files.exists(tagmanifest));
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path payloadFile = payloadDir.resolve(payload1.getFileName().toString());
        assertTrue(Files.exists(payloadFile));
        // assure completeness
        Bag bag = load(bagFile);
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test
    public void basicBagPartsPresentMultiChecksum() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("cs-bag6").toPath();
        new Filler(bagFile, "SHA-256", "SHA-512").payload(payload1).toDirectory();
        Path decl = bagFile.resolve(DECL_FILE);
        assertTrue(Files.exists(decl));
        Path manifest1 = bagFile.resolve("manifest-sha256.txt");
        assertTrue(Files.exists(manifest1));
        Path manifest2 = bagFile.resolve("manifest-sha512.txt");
        assertTrue(Files.exists(manifest2));
        Path tagmanifest1 = bagFile.resolve("tagmanifest-sha256.txt");
        assertTrue(Files.exists(tagmanifest1));
        Path tagmanifest2 = bagFile.resolve("tagmanifest-sha512.txt");
        assertTrue(Files.exists(tagmanifest2));
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path payloadFile = payloadDir.resolve(payload1.getFileName().toString());
        assertTrue(Files.exists(payloadFile));
        // assure completeness
        Bag bag = load(bagFile);
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    // tests for payload references (fetch.txt)
    @Test
    public void partsPresentFetchBag() throws IOException, URISyntaxException {
        Path bagFile = tempFolder.newFolder("ft-bag1").toPath();
        URI location = new URI("http://www.example.com/foo");
        new Filler(bagFile).payload("first.pdf", payload1).payloadRef("second/second.pdf", payload2, location).toDirectory();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path pload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(pload1));
        Path pload2 = payloadDir.resolve("second/second.pdf");
        assertTrue(!Files.exists(pload2));
        Path fetch = bagFile.resolve("fetch.txt");
        assertTrue(Files.exists(fetch));
         // assure incompleteness
        Bag bag = load(bagFile);
        assertTrue(!bag.isComplete());
    }

    @Test(expected = IOException.class)
    public void nonAbsoluteURIFetchBag() throws IOException, URISyntaxException {
        Path bagFile = tempFolder.newFolder("ft-bag2").toPath();
        URI location = new URI("/www.example.com/foo");
        new Filler(bagFile).payload("first.pdf", payload1).payloadRef("second/second.pdf", payload2, location).toDirectory();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path pload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(pload1));
        Path pload2 = payloadDir.resolve("second/second.pdf");
        assertTrue(!Files.exists(pload2));
        Path fetch = bagFile.resolve("fetch.txt");
        assertTrue(Files.exists(fetch));
         // assure incompleteness
        Bag bag = load(bagFile);
        assertTrue(!bag.isComplete());
    }

    @Test
    public void streamReadFetchBag() throws IOException, URISyntaxException {
        Path bagFile = tempFolder.newFolder("ft-bag3").toPath();
        URI location = new URI("http://www.example.com/foo");
        InputStream plIS = Files.newInputStream(payload1);
        new Filler(bagFile).payloadRef("second/second.pdf", plIS, location).toDirectory();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        Path pload2 = payloadDir.resolve("second/second.pdf");
        assertTrue(!Files.exists(pload2));
        Path fetch = bagFile.resolve("fetch.txt");
        assertTrue(Files.exists(fetch));
         // assure incompleteness
        Bag bag = load(bagFile);
        assertTrue(!bag.isComplete());
    }

    @Test
    public void partsPresentUnsafeFetchBag() throws IOException, URISyntaxException {
        Path bagFile = tempFolder.newFolder("ft-bag4").toPath();
        URI location = new URI("http://www.example.com/foo");
        Map<String, String> algs = Map.of("SHA-512", "0f9d6b52621011d46dabe200fd28ab35f48665e2de4c728ff1b26178d746419df3f43d51057337362f2ab987d8dbdffd8df1ff91c4d65777d77dea38b48cb4dd");
        new Filler(bagFile).payload("first.pdf", payload1).payloadRefUnsafe("second/second.pdf", 9070L, location, algs).toDirectory();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path pload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(pload1));
        Path pload2 = payloadDir.resolve("second/second.pdf");
        assertTrue(!Files.exists(pload2));
        Path fetch = bagFile.resolve("fetch.txt");
        assertTrue(Files.exists(fetch));
         // assure incompleteness
        Bag bag = load(bagFile);
        assertTrue(!bag.isComplete());
    }

    @Test
    public void multiPayloadBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag3").toPath();
        new Filler(bagFile).payload("first.pdf", payload1).payload("second/second.pdf", payload2).toDirectory();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path pload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(pload1));
        Path pload2 = payloadDir.resolve("second/second.pdf");
        assertTrue(Files.exists(pload2));
         // assure completeness
        Bag bag = load(bagFile);
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test
    public void multiTagBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag4").toPath();
        new Filler(bagFile).tag("first.pdf", tag1).tag("second/second.pdf", tag2).toDirectory();
        Path tagDir = bagFile.resolve("second");
        assertTrue(Files.isDirectory(tagDir));
        Path ttag1 = bagFile.resolve("first.pdf");
        assertTrue(Files.exists(ttag1));
        Path ttag2 = tagDir.resolve("second.pdf");
        assertTrue(Files.exists(ttag2));
         // assure completeness
        Bag bag = load(bagFile);
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test
    public void metadataBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag5").toPath();
        Filler filler = new Filler(bagFile).payload("first.pdf", payload1);
        String val1 = "metadata value";
        String val2 = "JUnit4 Test Harness";
        filler.metadata("Metadata-test", val1);
        filler.metadata(SOURCE_ORG, val2);
        Bag bag = load(filler.toDirectory());
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path payload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(payload1));
        assertTrue(bag.metadata("Metadata-test").get(0).equals(val1));
        assertTrue(bag.metadata(SOURCE_ORG).get(0).equals(val2));
    }

    @Test
    public void autoGenMetadataBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag6").toPath();
        Filler filler = new Filler(bagFile).payload("first.pdf", payload1);
        String val1 = "metadata value";
        String val2 = "JUnit4 Test Harness";
        filler.metadata("Metadata-test", val1);
        filler.metadata(SOURCE_ORG, val2);
        Bag bag = load(filler.toDirectory());
        Path payloadDir = bagFile.resolve("data");
        assertTrue(Files.isDirectory(payloadDir));
        Path payload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(payload1));
        assertNotNull(bag.metadata(BAGGING_DATE));
        assertNotNull(bag.metadata(BAG_SIZE));
        assertNotNull(bag.metadata(PAYLOAD_OXUM));
        assertNotNull(bag.metadata(BAG_SOFTWARE_AGENT));
        Path bagFile2 = tempFolder.newFolder("bag7").toPath();
        Filler filler2 = new Filler(bagFile2).payload("first.pdf", payload1);
        filler2.autoGen(new HashSet<>()).metadata(SOURCE_ORG, val2);
        Bag bag2 =  load(filler2.toDirectory());
        assertNull(bag2.metadata(BAGGING_DATE));
        assertNull(bag2.metadata(BAG_SIZE));
        assertNull(bag2.metadata(PAYLOAD_OXUM));
        Path bagFile3 = tempFolder.newFolder("bag7a").toPath();
        Filler filler3 = new Filler(bagFile3).payload("first.pdf", payload1);
        Set<Bag.MetadataName> names = new HashSet<>();
        names.add(BAG_SIZE);
        names.add(PAYLOAD_OXUM);
        filler3.autoGen(names);
        Bag bag3 = load(filler3.toDirectory());
        assertNull(bag3.metadata(BAGGING_DATE));
        assertNotNull(bag3.metadata(BAG_SIZE));
        assertNotNull(bag3.metadata(PAYLOAD_OXUM));
    }

    @Test
    public void completeAndIncompleteBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag8").toPath();
        Filler filler = new Filler(bagFile).payload("first.pdf", payload1).payload("second.pdf", payload2);
        Bag bag = load(filler.toDirectory());
        assertTrue(bag.isComplete());
        // now remove a payload file
        Path toDel = bagFile.resolve("data/first.pdf");
        Files.delete(toDel);
        assertTrue(!bag.isComplete());
    }

    @Test
    public void validAndInvalidBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag9").toPath();
        Filler filler = new Filler(bagFile).payload("first.pdf", payload1);
        Bag bag = load(filler.toDirectory());
        assertTrue(bag.isValid());
        // now remove a payload file
        Path toDel = bagFile.resolve("data/first.pdf");
        Files.delete(toDel);
        assertTrue(!bag.isValid());
    }

    @Test
    public void bagPackandLoadRoundtrip() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("bag10").toPath();
        Filler filler = new Filler(bagFile).payload("first.pdf", payload1);
        Path bagPackage = filler.toPackage();
        Bag bag = load(bagPackage);
        Path payload = bag.payloadFile("first.pdf");
        assertTrue(Files.size(payload1) == Files.size(payload));
    }

    @Test
    public void bagFileAttributesPreservedInZip() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("bag11").toPath();
        BasicFileAttributes beforeAttrs = Files.readAttributes(payload1, BasicFileAttributes.class);
        Filler filler = new Filler(bagFile).payload("first.pdf", payload1);
        // should preserve file time attrs if noTime false
        Path bagPackage = filler.toPackage("zip", false);
        Bag bag = load(bagPackage);
        Path payload = bag.payloadFile("first.pdf");
        BasicFileAttributes afterAttrs = Files.readAttributes(payload, BasicFileAttributes.class);
        // zip packages seem to lose millisecond precision in attributes, will agree in seconds
        assertTrue(beforeAttrs.creationTime().toInstant().truncatedTo(ChronoUnit.SECONDS)
        .compareTo(afterAttrs.creationTime().toInstant().truncatedTo(ChronoUnit.SECONDS)) == 0);
        assertTrue(beforeAttrs.lastModifiedTime().toInstant().truncatedTo(ChronoUnit.SECONDS)
        .compareTo(afterAttrs.lastModifiedTime().toInstant().truncatedTo(ChronoUnit.SECONDS)) == 0);
    }

    @Test
    public void bagFileAttributesPreservedInTGZ() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("bag11b").toPath();
        BasicFileAttributes beforeAttrs = Files.readAttributes(payload1, BasicFileAttributes.class);
        Filler filler = new Filler(bagFile).payload("first.pdf", payload1);
        // should preserve file time attrs if noTime false
        Path bagPackage = filler.toPackage("tgz", false);
        Bag bag = load(bagPackage);
        Path payload = bag.payloadFile("first.pdf");
        BasicFileAttributes afterAttrs = Files.readAttributes(payload, BasicFileAttributes.class);
        // zip packages seem to lose millisecond precision in attributes, will agree in seconds
        assertTrue(beforeAttrs.creationTime().toInstant().truncatedTo(ChronoUnit.SECONDS)
        .compareTo(afterAttrs.creationTime().toInstant().truncatedTo(ChronoUnit.SECONDS)) == 0);
        assertTrue(beforeAttrs.lastModifiedTime().toInstant().truncatedTo(ChronoUnit.SECONDS)
        .compareTo(afterAttrs.lastModifiedTime().toInstant().truncatedTo(ChronoUnit.SECONDS)) == 0);
    }

    @Test
    public void alternateArchiveNameTGZ() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("bag11c").toPath();
        Path altFile = tempFolder.newFolder("bag11c.tar.gz").toPath();
        Filler filler = new Filler(bagFile).payload("first.pdf", payload1);
        Path bagPackage = filler.toPackage("tgz", false);
        // copy to alternate form .tgz => .tar.gz
        Files.move(bagPackage, altFile, StandardCopyOption.REPLACE_EXISTING);
        Bag bag = load(altFile);
        Path payload = bag.payloadFile("first.pdf");
        assertTrue(Files.size(payload1) == Files.size(payload));
    }

    @Test
    public void bagFileAttributesClearedInZipNt() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("bag12").toPath();
        BasicFileAttributes beforeAttrs = Files.readAttributes(payload1, BasicFileAttributes.class);
        Filler filler = new Filler(bagFile).payload("first.pdf", payload1);
        // should strip file time attrs if noTime true
        Path bagPackage = filler.toPackage("zip", true);
        Bag bag = load(bagPackage);
        Path payload = bag.payloadFile("first.pdf");
        BasicFileAttributes afterAttrs = Files.readAttributes(payload, BasicFileAttributes.class);
        assertTrue(beforeAttrs.creationTime().compareTo(afterAttrs.creationTime()) != 0);
        assertTrue(beforeAttrs.lastModifiedTime().compareTo(afterAttrs.lastModifiedTime()) != 0);
    }

    @Test(expected = IllegalAccessException.class)
    public void sealedBagAccess() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("bag13").toPath();
        Filler filler = new Filler(bagFile).payload("first.pdf", payload1);
        Path bagPackage = filler.toPackage();
        Bag bag = seal(bagPackage);
        // stream access OK
        assertNotNull(bag.payloadStream("first.pdf"));
        // will throw IllegalAccessException
        Path payload = bag.payloadFile("first.pdf");
    }

    @Test
    public void streamReadBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag14").toPath();
        Filler filler = new Filler(bagFile);
        InputStream plIS = Files.newInputStream(payload1);
        InputStream tagIS = Files.newInputStream(tag1);
        filler.payload("first.pdf", plIS).tag("firstTag.txt", tagIS).toDirectory();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        Path pload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(pload1));
        Path ttag1 = bagFile.resolve("firstTag.txt");
        assertTrue(Files.exists(ttag1));
        // assure completeness
        Bag bag = load(bagFile);
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test
    public void streamWrittenPayloadBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag15").toPath();
        Filler filler = new Filler(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        plout.close();
        filler.toDirectory();
        // write the same to dup file
        Path dupFile = tempFolder.newFile("dupFile").toPath();
        OutputStream dupOut = Files.newOutputStream(dupFile);
        for (int i = 0; i < 1000; i++) {
            dupOut.write("lskdflsfevmep".getBytes());
        }
        dupOut.close();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        Path pload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(pload1));
        assertTrue(Files.size(pload1) == Files.size(dupFile));
         // assure completeness
        Bag bag = load(bagFile);
        try {
            Path pload2 = bag.payloadFile("first.pdf");
            assertTrue(Files.size(pload2) == Files.size(dupFile));
        } catch (Exception e) {}
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

     @Test
    public void streamFilePayloadParityBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag16").toPath();
        Filler filler = new Filler(bagFile);
        filler.payload("first.pdf", payload1);
        OutputStream dupOut = filler.payloadStream("second.pdf");
        // write the same as stream
        InputStream dupIn = Files.newInputStream(payload1);
        int read = dupIn.read();
        while(read != -1) {
            dupOut.write(read);
            read = dupIn.read();
        }
        dupIn.close();
        dupOut.close();
        Path bagDir = filler.toDirectory();
        Bag fullBag = load(bagDir);
        try {
            assertTrue(Files.size(fullBag.payloadFile("first.pdf")) == Files.size(fullBag.payloadFile("second.pdf")));
        } catch (Exception e) {}
        Map<String, String> manif = fullBag.payloadManifest("SHA-512");
        assertTrue(manif.get("data/first.pdf").equals(manif.get("data/second.pdf")));
    }

     @Test
    public void streamWrittenBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag17").toPath();
        Filler filler = new Filler(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        plout.close();
        OutputStream tout = filler.tagStream("tags/firstTag.txt");
        for (int i = 0; i < 1000; i++) {
            tout.write("lskdflsfevmep".getBytes());
        }
        tout.close();
        filler.toDirectory();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        Path pload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(pload1));
        Path ttag1 = bagFile.resolve("tags/firstTag.txt");
        assertTrue(Files.exists(ttag1));
        // assure completeness
        Bag bag = load(bagFile);
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test(expected = IOException.class)
    public void nonRenentrantFiller() throws IOException {
        // create transient filler
        Filler filler = new Filler().payload("first.pdf", payload1);
        InputStream in1 = filler.toStream();
        in1.close();
        // should throw exception here - closing in1 should delete bag
        InputStream in2 = filler.toStream();
        Path bagFile = tempFolder.newFolder("bag18").toPath();
        // should never reach this assert
        assertTrue(Files.notExists(bagFile));
    }

    @Test
    public void correctManifestSize() throws IOException {
        Path bagFile = tempFolder.newFolder("bag19").toPath();
        Filler filler = new Filler(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        filler.payload("second.pdf", payload1);
        Path fullBag = filler.toDirectory();
        // manifest should have 2 lines - one for each payload
        Path manif = fullBag.resolve("manifest-sha512.txt");
        assertTrue(lineCount(manif) == 2);
    }

    @Test
    public void correctManifestAPISize() throws IOException {
        Path bagFile = tempFolder.newFolder("bag20").toPath();
        Filler filler = new Filler(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        filler.payload("second.pdf", payload1);
        // without serialization, only non-stream payload available in manifest count
        assertTrue(filler.getManifest("SHA-512").size() == 1);
        // serialization required to flush payloadstream
        Path fullBag = filler.toDirectory();
        // manifest should have 2 lines - one for each payload
        assertTrue(filler.getManifest("SHA-512").size() == 2);
    }

    @Test
    public void correctTagManifestSize() throws IOException {
        Path bagFile = tempFolder.newFolder("bag21").toPath();
        Filler filler = new Filler(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        filler.payload("second.pdf", payload1);
        Path fullBag = filler.toDirectory();
        Path manif = fullBag.resolve("tagmanifest-sha512.txt");
        // should have a line for bagit.txt, bag-info.txt, and manifest*.txt
        assertTrue(lineCount(manif) == 3);
    }

    @Test
    public void streamCloseIndifferentManifest() throws IOException {
        Path bagFile = tempFolder.newFolder("bag22").toPath();
        Filler filler = new Filler(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Path fullBag = filler.toDirectory();
        // manifest should have 2 lines - one for each payload
        Path manif = fullBag.resolve("manifest-sha512.txt");
        assertTrue(lineCount(manif) == 2);
    }

    @Test
    public void loadedFromFileComplete() throws IOException {
        Path bagFile = tempFolder.newFolder("bag23").toPath();
        Filler filler = new Filler(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Path fullBag = filler.toPackage();
        // Load this bag from package file
        Bag loadedBag = load(fullBag);
        assertTrue(loadedBag.isComplete());
    }

    @Test
    public void loadedFromFileCSANotNull() throws IOException {
        Path bagFile = tempFolder.newFolder("bag24").toPath();
        Filler filler = new Filler(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Path fullBag = filler.toPackage();
        // Load this bag from package file
        Bag loadedBag = load(fullBag);
        assertTrue(loadedBag.csAlgorithms().size() > 0);
    }

    @Test
    public void loadedFromFileExpectedCSA() throws IOException {
        Path bagFile = tempFolder.newFolder("bag25").toPath();
        Filler filler = new Filler(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Path fullBag = filler.toPackage();
        // Load this bag from package file
        Bag loadedBag = load(fullBag);
        assertTrue(loadedBag.csAlgorithms().contains("SHA-512"));
    }

    @Test
    public void loadedFromFileValid() throws IOException {
        Path bagFile = tempFolder.newFolder("bag26").toPath();
        Filler filler = new Filler(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Path fullBag = filler.toPackage();
        // Load this bag from package file
        Bag loadedBag = load(fullBag);
        assertTrue(loadedBag.isValid());
    }

    @Test
    public void loadedFromDirectoryValid() throws IOException {
        Path bagFile = tempFolder.newFolder("bag27").toPath();
        Filler filler = new Filler(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Path bagDir = filler.toDirectory();
        // Load this bag from package file
        Bag loadedBag = load(bagDir);
        assertTrue(loadedBag.isValid());
    }

    @Test
    public void loadedFromStreamComplete() throws IOException {
        Path bagFile = tempFolder.newFolder("bag28").toPath();
        Filler filler = new Filler(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Path fullBag = filler.toPackage();
        // Load this bag from stream
        Bag loadedBag = load(Files.newInputStream(fullBag), "zip");
        assertNotNull(loadedBag.payloadManifest("SHA-512"));
        assertTrue(loadedBag.isComplete());
    }

    @Test
    public void loadedFromStreamToDirComplete() throws IOException {
        Path bagFile = tempFolder.newFolder("bag29").toPath();
        Filler filler = new Filler(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Path fullBag = filler.toPackage();
        // Load this bag from stream
        Path newBag = tempFolder.newFolder("bag30").toPath();
        Bag loadedBag = load(newBag, Files.newInputStream(fullBag), "zip");
        assertTrue(loadedBag.isComplete());
    }

    @Test
    public void loadedFromStreamValid() throws IOException {
        Path bagFile = tempFolder.newFolder("bag31").toPath();
        Filler filler = new Filler(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Path fullBag = filler.toPackage();
        // Load this bag from stream
        Bag loadedBag = load(Files.newInputStream(fullBag), "zip");
        assertTrue(loadedBag.isValid());
    }

    @Test
    public void defaultEOLInTextFiles() throws IOException {
        Path bagFile = tempFolder.newFolder("bag32").toPath();
        Filler filler = new Filler(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        Path fullBag = filler.toDirectory();
        // use bag-info.txt as representative text-file
        Path info = fullBag.resolve("bag-info.txt");
        // line ending is same as system-defined one
        assertTrue(load(fullBag).isValid());
        assertTrue(findSeparator(info).equals(System.lineSeparator()));
    }

    @Test
    public void unixEOLInTextFiles() throws IOException {
        Path bagFile = tempFolder.newFolder("bag33").toPath();
        Filler filler = new Filler(bagFile, StandardCharsets.UTF_8, Filler.EolRule.UNIX, false, "SHA-512");
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        Path fullBag = filler.toDirectory();
        // use bag-info.txt as representative text-file
        Path info = fullBag.resolve("bag-info.txt");
        // line ending is same as system-defined one
        assertTrue(load(fullBag).isValid());
        assertTrue(findSeparator(info).equals("\n"));
    }

    @Test
    public void windowsEOLInTextFiles() throws IOException {
        Path bagFile = tempFolder.newFolder("bag34").toPath();
        Filler filler = new Filler(bagFile, StandardCharsets.UTF_8, Filler.EolRule.WINDOWS, false, "SHA-512");
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        Path fullBag = filler.toDirectory();
        // use bag-info.txt as representative text-file
        Path info = fullBag.resolve("bag-info.txt");
        // line ending is same as system-defined one
        assertTrue(load(fullBag).isValid());
        assertTrue(findSeparator(info).equals("\r\n"));
    }

    @Test
    public void counterEOLInTextFiles() throws IOException {
        Path bagFile = tempFolder.newFolder("bag35").toPath();
        Filler filler = new Filler(bagFile, StandardCharsets.UTF_8, Filler.EolRule.COUNTER_SYSTEM, false, "SHA-512");
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        Path fullBag = filler.toDirectory();
        // use bag-info.txt as representative text-file
        Path info = fullBag.resolve("bag-info.txt");
        // line ending is not the same as system-defined one
        assertTrue(load(fullBag).isValid());
        assertTrue(! findSeparator(info).equals(System.lineSeparator()));
    }

    @Test
    public void validAndInvalidBagUTF16() throws IOException {
        Path bagFile = tempFolder.newFolder("bag36").toPath();
        Filler filler = new Filler(bagFile, StandardCharsets.UTF_16, "MD5").payload("first.pdf", payload1);
        Bag bag = load(filler.toDirectory());
        Map<String, String> tman = bag.tagManifest("MD5");
        assertTrue(tman.size() == 3);
        assertTrue(tman.keySet().contains("bagit.txt"));
        assertTrue(tman.keySet().contains("bag-info.txt"));
        assertTrue(tman.keySet().contains("manifest-md5.txt"));
        assertTrue(bag.isValid());
        // now remove a payload file
        Path toDel = bagFile.resolve("data/first.pdf");
        Files.delete(toDel);
        assertTrue(!bag.isValid());
    }

    @Test
    public void percentPathNamePayloadBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag37").toPath();
        new Filler(bagFile).payload("first%1.pdf", payload1).payload("second/second%2.pdf", payload2).toDirectory();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path pload1 = payloadDir.resolve("first%1.pdf");
        assertTrue(Files.exists(pload1));
        Path pload2 = payloadDir.resolve("second/second%2.pdf");
        assertTrue(Files.exists(pload2));
        // look in manifest files for encoded path names
        String manifLine = readTextLine(bagFile.resolve("manifest-sha512.txt"), 0);
        assertTrue(manifLine.endsWith("first%251.pdf"));
        String manifLine2 = readTextLine(bagFile.resolve("manifest-sha512.txt"), 1);
        assertTrue(manifLine2.endsWith("second%252.pdf"));
         // assure completeness
        Bag bag = load(bagFile);
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test
    public void percentPathNameFetchBag() throws IOException, URISyntaxException {
        Path bagFile = tempFolder.newFolder("bag38").toPath();
        URI location = new URI("http://www.example.com/foo");
        URI location2 = new URI("http://www.example.com/foo");
        new Filler(bagFile).payloadRef("first%1.pdf", payload1, location).payloadRef("second/second%2.pdf", payload2, location2).toDirectory();
        // look in fetch file for encoded path names
        String manifLine = readTextLine(bagFile.resolve("fetch.txt"), 0);
        assertTrue(manifLine.endsWith("first%251.pdf"));
        String manifLine2 = readTextLine(bagFile.resolve("fetch.txt"), 1);
        assertTrue(manifLine2.endsWith("second%252.pdf"));
    }

    private String readTextLine(Path file, int lineNum) throws IOException {
        final List<String> lines = Files.readAllLines(file);
        return lines.get(lineNum);
    }

    private String findSeparator(Path file) throws IOException {
        try (Scanner scanner = new Scanner(file)) {
            // it's one or the other
            return (scanner.findWithinHorizon("\r\n", 500) != null) ? "\r\n" : "\n";
        }
    }

    private int lineCount(Path file) throws IOException {
        Scanner scanner = new Scanner(file);
        int count = 0;
        while (scanner.hasNext()) {
            count++;
            scanner.nextLine();
        }
        scanner.close();
        return count;
    }
}
