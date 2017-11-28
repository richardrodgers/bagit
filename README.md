# Java BagIt Library #

This project contains a lightweight java library to support creation and consumption of BagIt-packaged content, as specified
by the BagIt IETF Draft Spec version 0.97. It requires a Java 8 or better JRE to run, has a single dependency on the Apache
commons compression library for support of tarred Gzip archive format (".tgz"), and is Apache 2 licensed. Build with Gradle or Maven.

[![Build Status](https://travis-ci.org/richardrodgers/bagit.svg?branch=master)](https://travis-ci.org/richardrodgers/bagit)
[![Dependency Status](https://dependencyci.com/github/richardrodgers/bagit/badge)](https://dependencyci.com/github/richardrodgers/bagit)

## Use Cases ##

The library attempts to simplify a few of the most common use cases/patterns involving bag packages.
The first (the _producer_ pattern) is where content is assembled and placed into a bag, and the bag is then serialized
for transport/hand-off to another component or system. The goal here is to ensure that the constructed bag is correct.
A helper class - _Filler_ - is used to orchestrate this assembly. Sequence: new Filler -> add content -> add more content -> serialize.
The second (the _consumer_ pattern) is where a bag serialization (or a loose directory) is given and must
be interpreted and validated for use. Here another helper class - _Loader_ - is used to deserialize.
Sequence: new Loader -> load serialization -> convert to Bag -> process contents. If you have more complex needs
in java, (e.g. support for multiple spec versions), you may wish to consider the [Library of Congress Java Library](https://github.com/LibraryOfCongress/bagit-java).

## Creating Bags (producer pattern) ##

A very simple 'fluent' builder interface is used to create bags, where content is added utilizing an object called
a _Filler_. For example, to create a bag with a few files (here the java.nio.file.Path instances 'file1', 'file2'):

    Filler filler = new Filler().payload(file1).payload(file2);

Metadata (in tag files, default: _bag-info.txt_) can be added in the same fluent manner:

    filler = filler.metadata("Contact-Name", "Joe Bloggs").metadata("Contact-Email", "bloggsj@gmail.com");

Since bags are often used to _transmit_ packaged content, we would typically next obtain a serialization of the bag:

    InputStream bagStream = filler.toStream();

This would be a very natural way to export bagged content to a network service. A few defaults are at work in
this invocation, e.g. the _toStream()_ method with no arguments uses the default package serialization, which is a zip
archive. To convert the same bag to use a compressed tar format:

    InputStream bagStream = filler.toStream("tgz");

We don't always want bag I/O streams - suppose we wish to obtain a bag archive file package instead:

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

If we wish to prevent manipulation of the hydrated bag (see below), we can seal it:

    Bag bag = new Loader(secureZipFile).seal();

For all the API details consult the [Javadoc](http://richardrodgers.github.io/bagit/javadoc/index.html)

## Portability ##

Bags are intended to be portable data containers, in that one should be able to write them on one operating system,
and read them on another. The spec contemplates this in specific ways, e.g. by allowing text files such as
'bag-info.txt' legally to have _either_ Unix-style line termination, or Windows-style. Tools operating on bags ought
to expect and tolerate this diversity, but do not always. The library provides some assistance here by allowing the user
to specify a preference when creating bags. Thus, if the context of use (lifecycle) for a set of bags is known to be in
a Windows environment, the library can be instructed to use Windows line termination for the generated text files in bags,
even if the bags are being generated on a Unix system. By default, the library will use the termination of the
operating system it is running on ('CR/LF' on Windows, '\n' on Unix and MacOS), but this can be overridden.
See the [Javadoc](http://richardrodgers.github.io/bagit/javadoc/index.html) for details.

## Archive formats ##

Bags are commonly serialized to standard archive formats such as ZIP. The library supports two archive formats:
'zip' and 'tgz' and a variant in each of these. If the variant is used, the library suppresses the file
creation/modification time attributes, in order that checksums of archives produced at different times
may accurately reflect only bag contents. That is, the checksum of a zipped bag (with no timestamp variant) is
time-of-archiving and filesystem-time-invariant, but content-sensitive. The variant is requested with an API call.

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
to this degree the bag contents are _tamper-proof_.

## Bagger on the command line ##

The library bundles a very simple command-line tool called _Bagger_ that exposes much of the API.
Sample invocation:

    java -cp <classpath> edu.mit.lib.bagit.Bagger fill newbag -p payloadFile -m Metadata-Name='metadata value'

The Bagger command-line tool can be conveniently run from a so-called _fat_ jar that is a build option.
Fat jars include all dependencies in a single executable jar (no classpath declaration required):

    java -jar bagit-all-x.y.jar validate mybag

### Download ###

The distribution jars are kept at [Bintray](https://bintray.com), so make sure that repository is declared.
Then (NB: using the most current version), for Gradle:

    compile 'edu.mit.lib:bagit:0.9'

or Maven:

    <dependency>
      <groupId>edu.mit.lib</groupId>
      <artifactId>bagit</artifactId>
      <version>0.9</version>
    </dependency>

in a standard pom.xml dependencies block.
