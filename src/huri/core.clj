(ns huri.core
  (:require [huri.schema :refer [defcoercer]]
            (plumbing [core :refer [distinct-fast map-vals safe-get for-map
                                    map-from-vals]]
                      [map :refer [safe-select-keys]])
            [clj-time.core :as t]
            [net.cgrand.xforms :as x]
            [clojure.data.priority-map :refer [priority-map-by]]
            [clojure.math.numeric-tower :refer [ceil expt round]]
            [cheshire.core :as json]
            [schema.core :as s]
            [clojure.java.io :as io])
  (:import org.joda.time.DateTime))

(s/set-fn-validation! true)

(defn mapply
  ([f]
   (map (partial apply f)))
  ([f coll & colls]
   (apply sequence (mapply f) coll colls)))

(defn ensure-seq
  [x]
  (if (sequential? x)
    x
    [x]))

(defn fsome
  [f]
  (fn [& args]
    (when (every? some? args)
      (apply f args))))

(defn transpose
  [m]
  (apply map vector m))

(s/defschema Coll (s/maybe (s/pred coll?)))

(s/defschema Pred (s/pred ifn?))

(s/defschema KeyFn (s/pred ifn?))

(s/defschema KeyCombinator {:combinator (s/pred ifn?)
                            :keyfns [KeyFn]})

(s/defschema Filters {KeyCombinator Pred})

(defcoercer ->keyfn KeyFn
  [x]
  (if (keyword? x)
    #(safe-get % x)
    x))

(defcoercer ->pred Pred
  [x]
  (cond                             
    (vector? x) #(apply (first x) % (rest x))
    (ifn? x) x
    :else (partial = x)))

(defcoercer ->key-combinator KeyCombinator
  [x]
  (if (s/check KeyCombinator x)
    {:combinator identity
     :keyfns [x]}
    x))

(defcoercer ->filters Filters
  [x]
  (if (map? x)
    x
    {identity x}))

(defn col
  ([k]
   (map (->keyfn k)))
  ([k df]
   (sequence (col k) df)))

(defn any-of
  [& keyfns]
  {:combinator some-fn
   :keyfns keyfns})

(defn every-of
  [& keyfns]
  {:combinator every-pred
   :keyfns keyfns})

(defn where
  [filters df]
  (into (empty df)
    (->> (for [[{:keys [combinator keyfns]} pred] (->filters filters)]
           (apply combinator (map (partial comp pred) keyfns)))
         (apply every-pred)
         filter)
    df))

(s/defn rollup
  ([groupfn f df :- Coll]
   (into (sorted-map) 
     (x/by-key (->keyfn groupfn) (comp (x/into []) (map f)))       	
     df))
  ([groupfn f keyfn df]
   (rollup groupfn (comp f (partial col keyfn)) df)))

(def rollup-vals (comp vals rollup))

(s/defn window
  ([f df]
   (window f identity df))
  ([f keyfn df]
   (window 1 f keyfn df))
  ([lag :- (s/constrained s/Int pos?) f keyfn df :- Coll]
   (let [xs (col keyfn df)]
     (map f (drop lag xs) xs))))

(s/defn size
  [df :- [(s/pred coll?)]]
  [(count df) (count (first df))])

(s/defn cols
  [df :- [{s/Any s/Any}]]
  (keys (first df)))

(s/defn summary
  ([f]
   (partial summary f))
  ([f df :- Coll]
   (for-map [[as [f k filters]] (map-vals ensure-seq f)]
     as (summary f (or k identity) (cond->> df filters (where filters)))))
  ([f keyfn df]
   (if (vector? f)
     (map #(summary % keyfn df) f)     
     (apply f (map #(col % df) (ensure-seq keyfn))))))

(defn update-cols
  [update-fns df]
  (map (apply comp (for [[k f] update-fns]
                     #(update % k f)))
       df))

(defn derive-cols
  [new-cols df]
  (map (apply comp (for [[k [f & cols]] (map-vals ensure-seq new-cols)]
                     (fn [row]
                       (assoc row k ((if cols
                                       (fn [m]
                                         (apply f (map #((->keyfn %) m) cols)))
                                       f)
                                     row)))))
       df))

(defn ->data-frame
  [cols xs]
  (if (and (not= (count cols) (count (first xs)))
           (some coll? (first xs)))    
    (->data-frame cols (map (partial mapcat ensure-seq) xs))
    (map (partial zipmap cols) xs)))

(defn select
  [cols df]
  (map #(safe-select-keys % cols) df))

(defn join
  [left right [lkey rkey] & {:keys [inner-join?]}]
  (let [left->right (comp (map-from-vals (->keyfn rkey) right)
                          (->keyfn lkey))]
    (for [row left
          :when (or (left->right row) (not inner-join?))]
      (merge row (left->right row)))))

(def count-where (comp count where))

(defn count-distinct
  ([df]
   (count (distinct-fast df)))
  ([keyfn df]
   (count-distinct (col keyfn df))))

(defn safe-divide
  [numerator & denominators]
  (when (or (and (seq denominators) (not-any? zero? denominators))
            (and (not (zero? numerator)) (empty? denominators)))
    (double (apply / numerator denominators))))

(s/defn sum
  ([df]
   (sum identity df))
  ([keyfn df :- Coll]
   (transduce (col keyfn) + df)))

(defn rate
  ([keyfn-a keyfn-b]
   (partial rate keyfn-a keyfn-b))
  ([keyfn-a keyfn-b df]
   (safe-divide (sum keyfn-a df)
                (sum keyfn-b df))))

(defn share
  ([filters]
   (partial share filters))
  ([filters df]
   (safe-divide (count-where filters df) (count df)))
  ([filters weightfn df]   
   (safe-divide (sum weightfn (where filters df))
                (sum weightfn df))))

(defn distribution
  ([df]
   (distribution identity df))
  ([keyfn df]
   (distribution keyfn (constantly 1) df))
  ([keyfn weightfn df]
   (when-let [norm (safe-divide (sum weightfn df))]     
     (into (priority-map-by >)
       (rollup keyfn (comp (partial * norm) sum) weightfn df)))))

(s/defn mean
  ([df]
   (mean identity df))
  ([keyfn df :- Coll]
   (some->> (transduce (col keyfn) x/avg df) double))
  ([keyfn weightfn df]
   (let [keyfn (->keyfn keyfn)]
     (rate #(* (keyfn %) (weightfn %)) weightfn df))))

(defn harmonic-mean
  ([df]
   (harmonic-mean identity df))
  ([keyfn df]
   (double (/ (count df) (sum (comp / keyfn) df)))))

(def cdf (comp (partial into (sorted-map))
               (partial reductions (fn [[_ acc] [k v]]
                                     [k (+ acc v)]))
               (partial sort-by key)
               distribution))

(s/defn smooth
  [window :- (s/constrained s/Int pos?) xs :- [s/Num]]
  (sequence (x/partition window 1 (x/reduce x/avg)) xs))

(defn growth
  [b a]
  (safe-divide (- b a) a)) 

(defn sample
  ([n xs]
   (sample n {} xs))
  ([n {:keys [replacement? fraction?]} xs]
   (into (empty xs)
     (take (if fraction?
             (* (count xs) n)
             n))
     (if replacement?
       (repeatedly (let [xs (vec xs)]
                     #(rand-nth xs)))
       (shuffle (seq xs))))))

(defn threshold
  [min-size xs]
  (when (>= (count xs) min-size)
    xs))

(defn decay
  [lambda t]
  (expt Math/E (- (* lambda t))))

(defn round-to
  [precision x]
  (let [scale (/ precision)]
    (/ (round (* x scale)) scale)))

(s/defn extent
  ([xs :- (s/constrained [s/Any] not-empty)]
   [(apply min xs) (apply max xs)])
  ([keyfn df]
   (extent (col keyfn df))))

(defn clamp
  ([[lower upper] x]
   (clamp lower upper x))
  ([lower upper x]
   (max (min x upper) lower)))

(defn quarter-of-year
  [dt]
  (ceil (/ (t/month dt) 3)))

(defn quarter
  [dt]
  (t/date-time (t/year dt) (inc (* (dec (quarter-of-year dt)) 4))))

(defn date
  [dt]
  (t/floor dt t/day))

(defn year-month
  [dt]
  (t/floor dt t/month))

(defn week-of-year
  [dt]
  (.getWeekOfWeekyear dt))

(defn week
  [dt]
  (t/minus (date dt) (t/days (dec (t/day-of-week dt)))))

(defn day-of-year
  [dt]
  (inc (t/in-days (t/interval (t/floor dt t/year) dt))))

(defn after?
  [this & that]
  (t/after? this (if (instance? org.joda.time.DateTime (first that))
                   (first that)
                   (apply t/date-time that))))

(defn before?
  [this & that]
  (t/before? this (if (instance? org.joda.time.DateTime (first that))
                    (first that)
                    (apply t/date-time that))))

(defn between?
  [this start end]
  (t/within? start end this))

(defn before-now?
  [dt]
  (t/before? dt (t/now)))

(defn after-now?
  [dt]
  (t/after? dt (t/now)))

(defn in?
  ([dt y]
   (= (t/year dt) y))
  ([dt y m]
   (= (year-month dt) (t/date-time y m))))

(defn spit-json
  [f x]
  (json/encode-stream x (io/writer f)))

(defn slurp-json
  [f]
  (json/decode-stream (io/reader f) true))
