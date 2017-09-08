# huri

> A long time ago, there was a girl named Huri. Huri’s mother loved her daughter very much, and she always praised Huri. Huri’s mother stopped everybody who passed. She would point at the chair where Huri was sitting. She would say, “That’s my beautiful, hardworking Huri!” Huri always sat outside. She sometimes took naps in her chair. Other times, she sang simple songs. And Huri’s mother praised her hard work and beauty. All of the people in the town listened to Huri’s mother speak of how hard Huri worked. But they were confused; no one ever saw Huri do any work. Soon, the people in town began to giggle when Huri’s mother praised Huri. When they saw her, they whispered, “Here comes Lazy Huri’s mother.” One day, a stranger came into the town. When Huri’s mother saw him, she told him about Huri. She said, “My daughter is very beautiful and hardworking.” The next day, the man visited the king; he was the king’s messenger. He told the king about the beautiful and hardworking girl. The king said, “My son, the prince, will marry this hardworking girl!” The very next week, the prince and Huri were married! Everybody thought that Huri’s mother had planned the whole thing. That is why she had lied about how hard Huri worked; she did it to trick the prince! People sometimes asked Huri’s mother if she had tricked the prince into marrying her daughter. But she never admitted it. She only smiled and winked.
>
> — Armenian folktale

__Huri__ is a Clojure library for the lazy data scientists. It consists of
* __huri.core__ a loose set of functions on vanilla Clojure collections that constitute an ad-hoc specification of a data frame; along with some utility and math functions.
* __huri.time__ time handling utilities built on top of [clj-time](https://github.com/clj-time/clj-time).
* __huri.io__ I/O utilites following the API (`slurp-x`, `spit-x`, `cast-fns`, ...) used by [Semantic CSV](https://github.com/metasoarous/semantic-csv)
* __huri.plot__ a DSL for plotting that compiles to R (ggplot2) meant to be used with [Gorilla REPL](http://gorilla-repl.org/)
* __huri.etl__ some light-weight ETL scaffolding built on top of [~~Prismatic~~Plumatic Graph](https://github.com/plumatic/plumbing)

## Status

Huri is still in flux. However it is already used extensively (and has been for some time) at [GoOpti](https://goopti.com), so it can be considered at least somewhat battle-tested.

## Design philosophy

I gave about motivation and design phiosophy behind Huri at ClojureD 2016: [video](https://www.youtube.com/watch?v=PSTSO8K80U4), [slides](http://www.slideshare.net/simonbelak/doing-data-science-with-clojure).

## Usage

Add this dependency to your project:

```clj
[huri "0.10.0-SNAPSHOT"]
```

To get the plots working make sure you have R installed, and on your path so it's accessible from the command line. If you can run Rscript from the command line, then you should be good to go. You will also need to have some libraries installed which you can do from R REPL with:
```r
install.packages("ggplot2")
install.packages("scales")
install.packages("grid")
install.packages("RColorBrewer")	
install.packages("ggrepel")
install.packages("svglite")
install.packages("directlabels")
```

## [Examples](http://viewer.gorilla-repl.org/view.html?source=github&user=sbelak&repo=huri&path=examples/examples.cljw)

## Huri likes playing with

* http://gorilla-repl.org/
* https://github.com/clj-time/clj-time
* https://github.com/plumatic/plumbing
* https://github.com/metasoarous/semantic-csv
* https://github.com/expez/superstring
* https://github.com/sbelak/tide
* https://github.com/bigmlcom/sampling

## For the future

* Interactive charts;
* Optimizing `->>` that rewrites code on the fly to do as much as possible in a single pass and use transducer fusion more extensively (intermediate results don't need to be end user consumable).


## Contributing

Feel free to submit a pull request.
If you're looking for things to help with, please take a look at the [GH issues](https://github.com/sbelak/huri/issues) page.
Contributing to the issues with comments, feedback, or requests is also greatly appreciated.


## License

Copyright © 2016 Simon Belak

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
