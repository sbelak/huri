# huri

> A long time ago, there was a girl named Huri. Huri’s mother loved her daughter very much, and she always praised Huri. Huri’s mother stopped everybody who passed. She would point at the chair where Huri was sitting. She would say, “That’s my beautiful, hardworking Huri!” Huri always sat outside. She sometimes took naps in her chair. Other times, she sang simple songs. And Huri’s mother praised her hard work and beauty. All of the people in the town listened to Huri’s mother speak of how hard Huri worked. But they were confused; no one ever saw Huri do any work. Soon, the people in town began to giggle when Huri’s mother praised Huri. When they saw her, they whispered, “Here comes Lazy Huri’s mother.” One day, a stranger came into the town. When Huri’s mother saw him, she told him about Huri. She said, “My daughter is very beautiful and hardworking.” The next day, the man visited the king; he was the king’s messenger. He told the king about the beautiful and hardworking girl. The king said, “My son, the prince, will marry this hardworking girl!” The very next week, the prince and Huri were married! Everybody thought that Huri’s mother had planned the whole thing. That is why she had lied about how hard Huri worked; she did it to trick the prince! People sometimes asked Huri’s mother if she had tricked the prince into marrying her daughter. But she never admitted it. She only smiled and winked.
>
> — Armenian folktale

__Huri__ is a Clojure library for the lazy data scientists. It consists of
* __huri.core__ a loose set of functions on vanilla Clojure collections that consiitute an ad-hoc specification of a data frame; along with some utility math and date-time functions.
* __huri.plot__ a DSL for plotting that compiles to R (ggplot2) meant to be used with [Gorilla REPL](gorilla-repl.org)
* __huri.etl__ some light-weight ETL scaffolding built on top of [~~Prismatic~~Plumatic Graph](https://github.com/plumatic/plumbing)

## Design philosophy

I gave about motivation and design phiosophy behind Huri at ClojureD 2016: [video](https://www.youtube.com/watch?v=PSTSO8K80U4), [slides](http://www.slideshare.net/simonbelak/doing-data-science-with-clojure).

## Usage

Add this dependency to your project:

```clj
[huri "0.3.0-SNAPSHOT"]
```

To get the plots working make sure you have R installed, and on your path so it's accessible from the command line. If you can run Rscript from the command line, then you should be good to go. You will also need to have some libraries installed which you can do from R REPL with:
```r
install.packages("ggplot2")
install.packages("scales")
install.packages("grid")
install.packages("RColorBrewer")
install.packages("ggrepel")
```

## Examples

(http://viewer.gorilla-repl.org/view.html?source=github&user=sbelak&repo=huri&path=examples/examples.cljw)[http://viewer.gorilla-repl.org/view.html?source=github&user=sbelak&repo=huri&path=examples/examples.cljw]

## For the future

* Interactive charts
* Optimizing `->>` that rewrites code on the fly to do as much as possible in a single pass and use transducer fusion more extnesively (intermediate results don't need to be end user consumable).


## License

Copyright © 2016 Simon Belak

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
