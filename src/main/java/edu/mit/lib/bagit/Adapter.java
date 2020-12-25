/**
 * Copyright 2013, 2014 MIT Libraries
 * SPDX-Licence-Identifier: Apache-2.0
 */

package edu.mit.lib.bagit;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static edu.mit.lib.bagit.Bag.*;
import static edu.mit.lib.bagit.Bag.MetadataName.*;

/**
 * Adapter class bundles convenience methods to facilitate alteration,
 * enhancement or other reuse of bags.
 */
public class Adapter {

    /**
     * Returns a new Filler (bag builder) instance using a temporary
     * directory to hold a transient bag prepopulated with the
     * contents of passed bag, and configured to use the
     * bag's encoding, checksum algorithms, and line termination.
     * 
     * @param bag the bag whose content to prepopulate into filler
     * @return filler a Bag builder
     * @throws IOException if file IO fails
     * @throws IllegalAccessException when sealed contents are accessed
     * @throws URISyntaxException when invalid URIs encountered
     */
    public static Filler copy(Bag bag) throws IOException, IllegalAccessException, URISyntaxException {
        var csAlgs = bag.csAlgorithms();
        var filler = new Filler(Files.createTempDirectory("bag"),
                                bag.tagEncoding(), bag.lineSeparator(), true,
                                csAlgs.toArray(String[]::new));
        copyBag(filler, bag, csAlgs);
        return filler;
    }

    /**
     * Returns a new Filler (bag builder) instance using passed
     * directory to hold a non-transient bag prepopulated with the
     * contents of passed bag, and configured to use the
     * bag's encoding, checksum algorithms, and line termination.
     * 
     * @param base the base directory in which to construct the bag
     * @param bag the bag whose content to prepopulate into filler
     * @return filler a Bag builder
     * @throws IOException if file IO fails
     * @throws IllegalAccessException when sealed contents are accessed
     * @throws URISyntaxException when invalid URIs encountered
     */
    public static Filler copy(Path base, Bag bag) throws IOException, IllegalAccessException, URISyntaxException {
        var csAlgs = bag.csAlgorithms();
        var filler = new Filler(base, bag.tagEncoding(), bag.lineSeparator(), false,
                                csAlgs.toArray(String[]::new));
        copyBag(filler, bag, csAlgs);
        return filler;
    }

    /**
     * Returns a new Filler (bag builder) instance using passed
     * directory to hold a non-transient bag prepopulated with the
     * contents of passed bag, and configured to use the passed
     * checksum algorithms, and the bag's encoding and line termination.
     * 
     * @param base the base directory in which to construct the bag
     * @param bag the bag whose content to prepopulate into filler
     * @param csAlgorithms one or more checksum argorithms to use
     * @return filler a Bag builder
     * @throws IOException if file IO fails
     * @throws IllegalAccessException when sealed contents are accessed
     * @throws URISyntaxException when invalid URIs encountered
     */
    public static Filler copy(Path base, Bag bag, String ... csAlgorithms) throws IOException, IllegalAccessException, URISyntaxException {
        // ensure we can handle refs - bail if not
        var algSet = new HashSet<String>(Arrays.asList(csAlgorithms));
        if (! bag.payloadRefs().isEmpty() && ! bag.csAlgorithms().containsAll(algSet)) {
            throw new IllegalStateException("Fetch file checksums absent for requested algorithms");
        }
        var filler = new Filler(base, bag.tagEncoding(), bag.lineSeparator(), false, csAlgorithms);
        copyBag(filler, bag, Set.of(csAlgorithms));
        return filler;
    }

    private static void copyBag(Filler filler, Bag bag, Set<String> newAlgs) 
        throws IOException, IllegalAccessException, URISyntaxException  {
        var manifs = new HashMap<String, Map<String, String>>();
        var csAlgs = bag.csAlgorithms();
        var csAlg = csAlgs.iterator().next();
        var refs = bag.payloadRefs();
        var autoGs = autoGens(csAlgs);
        var autoMs = autoMetas();
        if (! refs.isEmpty()) {
            for (String alg : csAlgs) {
                manifs.put(alg, bag.payloadManifest(alg));
            }
        }
        for (String relPath : bag.payloadManifest(csAlg).keySet()) {
            // filter out payloads in refs
            if (! refs.containsKey(relPath)) {
                // strip 'data/' path prefix
                var path = relPath.substring(5);
                filler.payload(path, bag.payloadFile(path));
            }
        }
        for (String relPath : bag.tagManifest(csAlg).keySet()) {
            // filter out automatically generated tag files
            if (! autoGs.contains(relPath)) {
                filler.tag(relPath, bag.tagFile(relPath));
            }
        }
        for (String relPath : refs.keySet()) {
            var refParts = refs.get(relPath).split("\\s+");
            var size = refParts[0].equals("-") ? -1L : Long.parseLong(refParts[0]);
            var checksums = new HashMap<String, String>();
            for (String alg : newAlgs) {
                checksums.put(alg, manifs.get(alg).get(relPath));
            }
            filler.payloadRefUnsafe(relPath, size, new URI(refParts[1]), checksums);
        }
        for (String name : bag.metadataNames()) {
            if (! autoMs.contains(name)) {
                // may be multiple values
                for (String value : bag.metadata(name)) {
                    filler.metadata(name, value);
                }
            }
        }
    }

    private static Set<String> autoGens(Set<String> csAlgs) {
        var gens = new HashSet<String>();
        gens.add(DECL_FILE);
        gens.add(META_FILE);
        for (String alg : csAlgs) {
            gens.add(MANIF_FILE + csAlgoName(alg) + ".txt");
            gens.add(TAGMANIF_FILE + csAlgoName(alg) + ".txt");
        }
        return gens;
    }

    private static Set<String> autoMetas() {
        return Set.of(BAGGING_DATE.getName(), BAG_SIZE.getName(),
                      PAYLOAD_OXUM.getName(), BAG_SOFTWARE_AGENT.getName());
    }
}
