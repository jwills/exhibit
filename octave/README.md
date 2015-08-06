exhibit-octave
==============

Module for Octave bindings within Exhibit.

This modules requires [JavaOctave](https://kenai.com/projects/javaoctave/pages/Home) to build, and the `octave` executable to present in `$PATH` to execute.

Minimal setup instructions for OSX are provided below:

```sh
# Install JavaOctave
$ hg clone https://hg.kenai.com/hg/javaoctave~source-code-repository javaoctave-src
$ cd javaoctave-src/javaoctave
$ mvn clean install

# Install Octave
# brew install octave
```
