# Java BagIt Library #

This project contains a lightweight java library to support creation and consumption of BagIt-packaged content, as specified
by the BagIt IETF Draft Spec version 0.97. It requires a Java 7 or better JRE to run, has a single dependency on the Apache
commons compression library for support of tarred Gzip archive format (".tgz"), and is Apache 2 licensed. Build with Gradle or Maven.

[![Build Status](https://travis-ci.org/richardrodgers/bagit.svg?branch=master)]
(https://travis-ci.org/richardrodgers/bagit)

## Use Cases ##

The library attempts to simplify a few of the most common use cases/patterns involving bag packages.
The first (the _producer_ pattern) is where content is assembled and placed into a bag, and the bag is then serialized
for transport/hand-off to another component or system. The goal here is to ensure that the constructed bag is correct.
A helper class - bag _Filler_ - is used to orchestrate this assembly. Sequence: new Filler -> add content -> add more content -> serialize.
The second (the _consumer_ pattern) is where a bag serialization (or a loose directory) is given and must
be interpreted and validated for use. Here another helper class _Loader_ is used to deserialize.
Sequence: new Loader -> load serialization -> convert to Bag -> process contents. If you have more complex needs
in java, (e.g. support for multiple spec versions), you may wish to consider the [Library of Congress Java Library](https://github.com/anu-doi/bagit).

## Creating Bags (producer pattern) ##

A very simple 'fluent' builder interface is used to create bags, where content is added utilizing an object called
a _Filler_. For example, to create a bag with a few files (here the java.nio.file.Path File instances 'file1', 'file2'):

    Filler filler = new Filler().payload(file1).payload(file2);

Since bags are often used to _transmit_ packaged content, we would typically next obtain a serialization of the bag:

    InputStream bagStream = filler.toStream();

This would be a very natural way to export bagged content to a network service. A few defaults are at work in
this invocation, e.g. the 'toStream()' method with no arguments uses the default package serialization, which is a zip
archive. To convert the same bag to use a compressed tar format:

    InputStream bagStream = filler.toStream("tgz");

We don't always need bag I/O streams - suppose we wish obtain a reference to an archive file object instead:

    Path bagPackage = new Filler().payload(file1).metadata("External-Identifier", "mit.edu.0001").toPackage();

Another default in use so far has been that the Filler constructor (_new Filler()_) is not given a directory path for the bag.
In this case, a temporary directory for the bag is created. This has several implications, depending on how the filler
is later used.  If a stream is requested (as in the first example above), the temporary bag will be automatically deleted as
soon as the reader stream is closed. This is very convenient when used to transport bags to a network service - no clean-up is required:

    InputStream tempStream = new Filler().payload(myPayload).toStream();
    // consume stream
    tempStream.close();

If a package or directory is requested from the Filler (as opposed to a stream), the bag directory or file returned will be
deleted upon JVM exit only, which means that bag storage management could become an issue for a large number of
files and/or a long-running JVM. Thus good practice would be to either: have the client copy the bag package/directory
to a new location for long term preservation if desired, or timely client deletion of the package/directory so storage
is not taxed.

On the other hand, if a directory is passed to the Filler in the constructor, it will _not_ be considered temporary
and thus not be removed on stream close or JVM exit.

For example, can choose to access the bag contents as an (unserialized) directory in the file system comprising the bag.
In this case we need to indicate where we want to put the bag when we construct it:

    Path bagDir = new Filler(myDir).payload(file1).
                  payloadRef("file2", 20000, http://www.example.com/data.0002").toDirectory();

## Reading Bags (consumer pattern) ##

The reverse situation occurs when we wish to read or consume a bag. Here we are given a specific representation of
a purported bag, (viz. archive, I/O stream), and need to interpret it (and possibly validate it). The companion object
in this case is the 'Loader', which is used to produce Bag instances. Thus:

    Bag bag = new Loader(myZipFile).load();
    Path myBagFile = bag.payloadFile("firstSet/firstFile");

Or the bag contents may be obtained from a network stream:

    String bagId = new Loader(inputStream, "zip").load().metadata("External-Identifier");

## Archive formats ##

Bags are commonly serialized to standard archive formats such as ZIP. The library supports two archive formats:
'zip' and 'tgz' and the variants 'zip.nd' and 'tgz.nd' (no date). If these variants are used, the library
suppresses the file modification time attribute, in order that checksums of archives produced at different times
may accurately reflect only bag contents. That is, the checksum of a zip.nd bag is time-of-archiving and file
system-time-invariant, but content-sensitive.

## Extras ##

The library supports a few features not required by the BagIt spec. One is basic automatic
metadata generation. There are a small number of reserved properties typically recorded in _bag-info.txt_
that can easily be determined by the library. These values are automatically populated in _bag-info.txt_ by default.
The 'auto-fill' properties are: Bagging-Date, Bag-Size, Payload-Oxum, and one non-reserved property 'Bag-Software-Agent'.
If automatic generation is not desired, an API call disables it.

Another extra is _sealed_ bags. Bags created by Loaders are immutable, meaning they cannot be altered via the API.
But we typically _can_ gain access to the backing bag storage, which we can of course then
change at will. However, if a bag is created as _sealed_ (a method on the Loader), all
method calls that expose the underlying storage will throw IllegalAccess exceptions. So, for example,
we would be _unable_ to obtain a File reference, but _could_ get an I/O stream to the same content.
In other words, the content can be accessed, but the underlying representation cannot be altered, and
to this degreee the bag contents are _tamper-proof_.

## Bagger on the command line ##

The library bundles a very simple command-line tool called _Bagger_ that exposes much of the API.
Sample invocation:

    java Bagger fill newbag -p payloadFile -m Metadata-Name='metadata value'

### Filler API Details ###

NB: For a complete run-down, generate the javadoc for the package.

Constructors:

    // create a Filler using temporary backing directory and default checksum algorithm
    Filler()
    // create a Filler putting bag in passed directory and default checksum algorithm
    Filler(Path file)
    // create a Filler putting bag in passed directory with passed checkSum algorithm
    Filler(Path file, String csAlgorithm)

Methods for adding payload content:

    // copy payload file to root directory ('data') using it's name
    filler.payload(Path file)
    // copy payload file to relative path under data directory
    filler.payload(String relPath, Path file)
    // write payload stream to relative path under data directory
    filler.payload(String relPath, InputStream stream)
    // add a file URL reference to fetch.txt
    filler.payloadRef(String relPath, log size, String url)
    // obtain a writer stream to a relative path under data directory
    OutputStream filler.payloadStream(String relPath)

Methods for adding metadata:

    // set a metadata value for a reserved property
    filler.metadata(MetadataName mdName, String value)
    // set a metadata value for the named property
    filler.metadata(String name, String value)
    // set a metadata value in a named tagFile of the named property
    filler.metadata(String relPath, String name, String value)

Methods for adding tag (metadata) files

    // write tag file to relative path in bag
    filler.tag(String relPath, Path file)
    // write tag stream to relative path in bag
    filler.tag(String relPath, InputStream stream)
    // obtain a writer to a relative path  in bag
    OutputStream filler.tagStream(String relPath)

Methods for manifesting bags:

    // create bag in loose directory from Filler data
    Path filler.toDirectory()
    // create bag in archive package from Filler data
    Path filler.toPackage()
    // create bag I/O stream from Filler data
    InputStream filler.toStream()
    // obtain manifest data without manifesting bag
    List<String> getManifest()

### Loader API Details ###

Constructors:

    // create a Loader from contents of directory or archive file
    Loader(Path file)
    // create a Loader from contents of input stream of expected package format
    Loader(InputStream stream, String format)
    // create a Loader at specified directory from contents of input stream of expected package format
    Loader(Path file, InputStream stream, String format)

Methods for instantiating bags:

    // create bag from Loader data
    Bag loader.load()
    // create sealed (tamper-proof) bag from Loader data
    Bag loader.seal()

### Bag API Details ###

Methods for bag definition and status:

    // name of the bag
    String bag.bagName()
    // version of the BagIt spec bag adheres to
    String Bag.bagItVersion()
     // version of the library used to make bag
    String Bag.libVersion()
    // is bag complete?
    boolean bag.isComplete()
    // is bag sealed?
    boolean bag.isSealed()
    // is the bag valid?
    boolean bag.isValid()

Methods to obtain payload data:

    // get a payload file
    Path bag.payloadFile(String relPath)
    // get an InputStream to a payload file
    InputStream bag.payloadStream(String relPath)
    // get payload references
    Map<String, String> bag.payloadRefs()

Methods to obtain tag data:

    // get a tag file
    Path bag.tagFile(String relPath)
    // get an InputStream to a tag file
    InputStream bag.tagStream(String relPath)

Methods to obtain tag metadata:

    // get metadata value(s) for a reserved property
    List<String> bag.metadata(MetadataName mdName)
    // get metadata value(s) for the named property
    List<String> bag.metadata(String name)
    // get metadata value(s) from a named tag on relative path of the named property
    List<String> bag.metadata(String relPath, String name)
