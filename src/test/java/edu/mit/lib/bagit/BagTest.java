/**
 * Copyright 2013, 2014 MIT Libraries
 * Licensed under: http://www.apache.org/licenses/LICENSE-2.0
 */
package edu.mit.lib.bagit;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

/*
 * Basic unit tests for BagIt Library. Incomplete.
 */ 

@RunWith(JUnit4.class)
public class BagTest {

    public Path payload1, payload2, tag1, tag2;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

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

    @Test
    public void basicBagPartsPresentMD5() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("bag1").toPath();
        new Filler(bagFile).payload(payload1).toDirectory();
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
        Bag bag = new Loader(bagFile).load();
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test
    public void basicBagPartsPresentSHA1() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("bag2").toPath();
        new Filler(bagFile, "SHA1").payload(payload1).toDirectory();
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
        Bag bag = new Loader(bagFile).load();
        assertTrue(bag.isComplete());
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
    }

    @Test
    public void metadataBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag5").toPath();
        Filler filler = new Filler(bagFile).payload("first.pdf", payload1);
        String val1 = "metadata value";
        String val2 = "JUnit4 Test Harness";
        filler.metadata("Metadata-test", val1);
        filler.metadata(SOURCE_ORG, val2);
        Bag bag = new Loader(filler.toDirectory()).load();
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
        Bag bag = new Loader(filler.toDirectory()).load();
        Path payloadDir = bagFile.resolve("data");
        assertTrue(Files.isDirectory(payloadDir));
        Path payload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(payload1));
        assertNotNull(bag.metadata(BAGGING_DATE));
        assertNotNull(bag.metadata(BAG_SIZE));
        assertNotNull(bag.metadata(PAYLOAD_OXNUM));
        Path bagFile2 = tempFolder.newFolder("bag7").toPath();
        Filler filler2 = new Filler(bagFile2).payload("first.pdf", payload1);
        filler2.noAutoGen().metadata(SOURCE_ORG, val2);
        Bag bag2 = new Loader(filler2.toDirectory()).load();
        assertNull(bag2.metadata(BAGGING_DATE));
        assertNull(bag2.metadata(BAG_SIZE));
        assertNull(bag2.metadata(PAYLOAD_OXNUM));
    }

    @Test
    public void completeAndIncompleteBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag8").toPath();
        Filler filler = new Filler(bagFile).payload("first.pdf", payload1).payload("second.pdf", payload2);
        Bag bag = new Loader(filler.toDirectory()).load();
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
        Bag bag = new Loader(filler.toDirectory()).load();
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
        Bag bag = new Loader(bagPackage).load();
        Path payload = bag.payloadFile("first.pdf");
        assertTrue(Files.size(payload1) == Files.size(payload));
    }

    @Test(expected = IllegalAccessException.class)
    public void sealedBagAccess() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("bag11").toPath();
        Filler filler = new Filler(bagFile).payload("first.pdf", payload1);
        Path bagPackage = filler.toPackage();
        Bag bag = new Loader(bagPackage).seal();
        // stream access OK
        assertNotNull(bag.payloadStream("first.pdf"));
        // will throw IllegalAccessException
        Path payload = bag.payloadFile("first.pdf");
    }

    @Test
    public void streamReadBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag12").toPath();
        Filler filler = new Filler(bagFile);
        InputStream plIS = Files.newInputStream(payload1);
        InputStream tagIS = Files.newInputStream(tag1);
        filler.payload("first.pdf", plIS).tag("firstTag.txt", tagIS).toDirectory();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        Path pload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(pload1));
        Path ttag1 = bagFile.resolve("firstTag.txt");
        assertTrue(Files.exists(ttag1));
    }

    @Test
    public void streamWrittenBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag13").toPath();
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
    }

    @Test(expected = IOException.class)
    public void nonRenentrantFiller() throws IOException {
        // create transient filler
        Filler filler = new Filler().payload("first.pdf", payload1);
        InputStream in1 = filler.toStream();
        in1.close();
        // should throw exception here - closing in1 should delete bag
        InputStream in2 = filler.toStream();
        Path bagFile = tempFolder.newFolder("bag14").toPath();
        // should never reach this assert
        assertTrue(Files.notExists(bagFile));
    }
}

