(ns huri.core
  (:require (plumbing [core :refer [distinct-fast map-vals safe-get for-map
                                    map-from-vals map-from-keys]])
            [net.cgrand.xforms :as x]
            [clojure.data.priority-map :refer [priority-map-by]]            
            [clj-time.core :as t]
            [clojure.core.reducers :as r]
            [clojure.spec :as s]
            [clojure.spec.test :as s.test])
  (:import org.joda.time.DateTime))

(defn papply
  "partial that applies its arguments."
  [f & args]
  (apply partial apply f args))

(defn pcomp
  ([f g]
   (fn [& args]
     (let [intermediate (apply g args)]
       (if (fn? intermediate)
         (pcomp f intermediate)
         (f intermediate)))))
  ([f g & fs]
   (reduce pcomp (list* f g fs))))

(defn mapply
  "map that applies f via apply."
  ([f]
   (map (papply f)))
  ([f coll]
   (sequence (mapply f) coll)))

(defn mapm
  ""
  [f coll]
  (into {} (map f) coll))

(defn juxtm
  [m]
  (fn [& args]
    (map-vals #(apply % args) m)))

(defmacro for-cat
  [& for-body]
  `(apply concat (for ~@for-body)))

(defmacro for-keep
  [& for-body]
  `(remove nil? (for ~@for-body)))

(defmacro with-conformer
  [x & tagvals]
  `(s/conformer (fn [[tag# ~x]]
                  (case tag# ~@tagvals))))

(defmacro parallel-map
  "A hash map constructor that evaluates its arguments in parallel."
  [& keyvals]
  `(apply hash-map (pvalues ~@keyvals)))

(defn fsome
  "Takes a function f and returns a function that gets called only if all
the arguments passed in are not nill. Else returns nil."
  [f]
  (fn [& args]
    (when (every? some? args)
      (apply f args))))

(def patch-nil
  "Replaces nil with patch value, passes other values through."
  (partial fnil identity))

(def transpose
  "Transposes vector of vectors."
  (papply map vector))

(defn val-or-seq
  [element-type]
  (s/and
   (s/or :seq (s/coll-of element-type)
         :val element-type)
   (with-conformer x
     :seq x
     :val [x])))

(def ensure-seq (partial s/conform (val-or-seq any?)))

(def flatten1 (partial mapcat ensure-seq))

(s/def ::keyfn (s/and
                (s/or :fn (s/and ifn? (complement (some-fn keyword? vector?)))
                      :key (constantly true))
                (with-conformer x
                  :fn x
                  :key #(safe-get % x))))

(def ->keyfn (partial s/conform ::keyfn))

(s/def ::dataframe (s/nilable (s/every map?)))

(defn col
  ([k]
   (map (->keyfn k)))
  ([k df]
   (sequence (col k) df)))

(s/def ::col-transforms
  (s/map-of keyword?
            (s/and (s/or :vec (s/and vector?
                                     (s/cat :f ifn? :keyfns (s/+ ::keyfn)))
                         :ifn ifn?)
                   (with-conformer x
                     :vec (comp (papply (:f x)) (apply juxt (:keyfns x)))
                     :ifn x))))

(s/fdef derive-cols
  :args (s/cat :new-cols ::col-transforms
               :df ::dataframe)
  :ret ::dataframe)

(defn derive-cols
  [new-cols df]
  (map (->> new-cols
            (s/conform ::col-transforms)
            (map (fn [[ks f]]
                   (fn [row]
                     (assoc row ks (f row)))))
            (apply comp))
       df))

(defn update-cols
  [update-fns df]
  (derive-cols (for-map [[k f] update-fns]
                 k (comp f k))
               df))

(defn any-of
  [& keyfns]
  {::combinator some-fn
   ::keyfns keyfns})

(defn every-of
  [& keyfns]
  {::combinator every-pred
   ::keyfns keyfns})

(s/def ::keyfns (s/+ (val-or-seq ::keyfn)))

(s/def ::key-combinator (s/and
                         (s/or :combinator (s/keys :req [::combinator ::keyfns])
                               :keyfns (val-or-seq ::keyfn))
                         (with-conformer x
                           :combinator x
                           :keyfns {::combinator identity
                                    ::keyfns [x]})))

(s/def ::pred (s/and
               (s/or :vec (s/and vector? (s/cat :f ifn? :args (s/* any?)))
                     :fn ifn?
                     :val (complement ifn?))
               (with-conformer x
                 :vec #(apply (:f x) (concat % (:args x)))
                 :fn (papply x)
                 :val (papply = x))))

(s/def ::filters (s/and
                  (s/or :map (s/map-of ::key-combinator ::pred :conform-keys true)
                        :pred ::keyfn)
                  (with-conformer x
                    :map (->> x
                              (map (fn [[{:keys [::combinator ::keyfns]} pred]]
                                     (->> keyfns
                                          (map #(comp pred (apply juxt %)))
                                          (apply combinator))))
                              (apply every-pred))
                    :pred x)))

(s/fdef where
  :args (s/alt :curried ::filters
               :full (s/cat :filters ::filters
                            :df (s/nilable coll?)))
  :ret coll?)

(defn where
  ([filters]
   (partial where filters))
  ([filters df]
   (if (or (instance? clojure.lang.PersistentList df)
           (instance? clojure.lang.LazySeq df))
     (filter (s/conform ::filters filters) df)
     (into (empty df) (filter (s/conform ::filters filters)) df))))

(s/fdef summarize
  :args (s/alt :curried ::summary-fn
               :simple (s/cat :f ::summary-fn
                              :df (s/nilable coll?))
               :keyfn (s/cat :f ::summary-fn
                             :keyfn (val-or-seq ::keyfn)
                             :df (s/nilable coll?)))
  :ret any?)

(s/def ::summary-fn
  (s/or :map (s/map-of keyword? (s/and
                                 (s/or :vec (s/cat :f ifn?
                                                   :keyfn (val-or-seq ::keyfn)
                                                   :filters (s/? ::filters))
                                       :fn fn?)
                                 (with-conformer x
                                   :vec x
                                   :fn {:f x
                                        :keyfn [identity]})))
        :fn fn?))

(defn summarize
  ([f]
   (partial summarize f))
  ([f df]
   (summarize f identity df))
  ([f keyfn df]
   (let [[tag f] (s/conform ::summary-fn f)]
     (if (= tag :map)
       (into {}
         (pmap (fn [[k {f :f keyfn-local :keyfn filters :filters}]]
                 [k (summarize f (cond
                                   (= keyfn identity) keyfn-local
                                   (= keyfn-local [identity]) keyfn
                                   :else (map #(comp % keyfn) keyfn-local))
                               (if filters
                                 (where filters df)
                                 df))])
               f))
       (apply f (map #(col % df) (ensure-seq keyfn)))))))

(s/fdef rollup
  :args (s/alt :curried (s/cat :groupfn ::keyfn
                               :f ::summary-fn)
               :simple (s/cat :groupfn ::keyfn
                              :f ::summary-fn
                              :df (s/nilable coll?))
               :keyfn (s/cat :groupfn ::keyfn
                             :f ::summary-fn
                             :keyfn ::keyfn
                             :df (s/nilable coll?)))
  :ret (s/and map? sorted?))

(defn rollup
  ([groupfn f]
   (partial rollup groupfn f))
  ([groupfn f df]
   (rollup groupfn f identity df))
  ([groupfn f keyfn df]
   (into (sorted-map) 
     (x/by-key (->keyfn groupfn) (comp (x/into [])
                                       (map (partial summarize f keyfn))))
     df)))

(def rollup-vals (pcomp vals rollup))
(def rollup-keep (pcomp (partial remove nil?) rollup-vals))
(def rollup-cat (pcomp (papply concat) rollup-vals))

(s/def ::fuse-fn (s/and (s/or :map map?
                              :vec sequential?
                              :kw keyword?
                              :fn ifn?)
                        (with-conformer x
                          :map (map-vals ->keyfn x)
                          :vec (zipmap x x)
                          :kw {x (->keyfn x)}
                          :fn {::group x})))

(s/fdef rollup-fuse
  :args (s/alt :curried (s/cat :groupfn ::fuse-fn
                               :f ::summary-fn)
               :simple (s/cat :groupfn ::fuse-fn
                              :f ::summary-fn
                              :df (s/nilable coll?))
               :keyfn (s/cat :groupfn ::fuse-fn
                             :f ::summary-fn
                             :keyfn ::keyfn
                             :df (s/nilable coll?)))
  :ret coll?)

(defn rollup-fuse
  ([groupfn f]
   (partial rollup-fuse groupfn f))
  ([groupfn f df]
   (rollup-fuse groupfn f identity df))
  ([groupfn f keyfn df]
   (let [groupfn (s/conform ::fuse-fn groupfn)]
     (rollup-vals (apply juxt (vals groupfn))
                  (fn [group]
                    (merge (into {} (summarize f keyfn group))
                           ((juxtm groupfn) (first group))))
                  df))))

(s/fdef rollup-transpose
  :args (s/alt :curried (s/cat :indexfn ::keyfn
                               :f (s/and ::summary-fn map?))
               :full (s/cat :indexfn ::keyfn
                            :f (s/and ::summary-fn map?)
                            :df (s/nilable coll?)))
  :ret map?)

(defn rollup-transpose
  ([indexfn f]
   (partial rollup-transpose indexfn f))
  ([indexfn f df]
   (->> df
        (rollup indexfn f)
        (reduce-kv (fn [acc idx kvs]
                     (reduce-kv (fn [acc k v]
                                  (update acc k conj [idx v]))
                                acc
                                kvs))
                   (zipmap (keys f) (repeat (sorted-map)))))))

(s/fdef window
  :args (s/alt :curried ifn?
               :simple (s/cat :f ifn?
                              :df (s/nilable coll?))
               :keyfn (s/cat :f ifn?
                             :keyfn ::keyfn
                             :df (s/nilable coll?))
               :lag (s/cat :lag pos-int?
                           :f ifn?
                           :keyfn ::keyfn
                           :df (s/nilable coll?)))
  :ret coll?)

(defn window
  ([f]
   (partial window f))
  ([f df]
   (window f identity df))
  ([f keyfn df]
   (window 1 f keyfn df))
  ([lag f keyfn df]
   (let [xs (col keyfn df)]
     (map f (drop lag xs) xs))))

(s/fdef size
  :args (s/cat :df (s/every coll?))
  :ret (s/cat :rows int? :cols int?))

(defn size
  [df]
  [(count df) (count (first df))])

(s/fdef cols
  :args (s/cat :df ::dataframe)
  :ret coll?)

(defn cols
  [df]
  (keys (first df)))

(defn col-oriented
  [df]
  (for-map [k (cols df)]
    k (col k df)))

(defn row-oriented
  [m]
  (apply map (comp (partial zipmap (keys m)) vector) (vals m)))

(defn ->data-frame
  [cols xs]
  (if (and (not= (count cols) (count (first xs)))
           (some coll? (first xs)))    
    (->data-frame cols (map flatten1 xs))
    (map (partial zipmap cols) xs)))

(defn select-cols
  [cols df]
  (map (juxtm (map-from-keys ->keyfn cols)) df))

(s/def ::join-on (s/and
                  (s/or :vec (s/cat :left ::keyfn :right ::keyfn)
                        :singleton ::keyfn)
                  (with-conformer x
                    :vec x
                    :singleton {:left x :right x})))

(s/def ::op #{:inner-join :semi-join :anti-join :left-join})

(s/fdef join
  :args (s/alt :default (s/cat :on ::join-on
                               :left ::dataframe
                               :right ::dataframe)
               :with-op (s/cat :op ::op
                               :on ::join-on
                               :left ::dataframe
                               :right ::dataframe))
  :ret ::dataframe)

(defn join
  ([on left right]
   (join :left-join on left right))
  ([op on left right]
   (let [{lkey :left rkey :right} (s/conform ::join-on on)
         left->right (comp (map-from-vals rkey right) lkey)]
     (if (#{:semi-join :anti-join} op)
       (where (if (= op :semi-join)
                left->right
                (comp nil? left->right))
              left)
       (doall
        (for [row left
              :when (or (= op :left-join) (left->right row))]
          (merge row (left->right row))))))))

(defn count-where
  ([filters]
   (partial count-where filters))
  ([filters df]
   (count (where filters df))))

(defn count-distinct
  ([df]
   (count (distinct-fast df)))
  ([keyfn df]
   (count-distinct (col keyfn df))))

(defn safe-divide
  [numerator & denominators]
  (when (or (and (not-empty denominators) (not-any? zero? denominators))
            (and (not (zero? numerator)) (empty? denominators)))
    (double (apply / numerator denominators))))

(s/fdef sum
  :args (s/alt :coll (s/nilable coll?)
               :keyfn (s/cat :keyfn ::keyfn
                             :df (s/nilable coll?)))
  :ret number?)

(defn sum
  ""
  ([df]
   (sum identity df))
  ([keyfn df]
   (transduce (keep (->keyfn keyfn)) + df)))

(s/fdef rate
  :args (s/alt :curried (s/cat :keyfn-a ::keyfn
                               :keyfn-b ::keyfn)
               :full (s/cat :keyfn-a ::keyfn
                            :keyfn-b ::keyfn
                            :df (s/nilable coll?)))
  :ret (s/nilable number?))

(defn rate
  "Returns the quotient of the sum of values extracted by keyfn-a and 
  keyfn-b.
  Returns a curried version when only keyfns are provided."
  ([keyfn-a keyfn-b]
   (partial rate keyfn-a keyfn-b))
  ([keyfn-a keyfn-b df]
   (let [keyfn-a (->keyfn keyfn-a)
         keyfn-b (->keyfn keyfn-b)]
     (transduce identity
                (fn
                  ([] [0 0])
                  ([[sa sb :as acc] e]
                   (let [a (keyfn-a e)
                         b (keyfn-b e)]
                     (if (or (nil? a) (nil? b))
                       acc
                       [(+ a sa) (+ b sb)])))
                  ([[a b]]
                   (safe-divide a b)))
                df))))

(s/fdef share
  :args (s/alt :curried ::filters
               :simple (s/cat :filters ::filters
                              :df (s/nilable coll?))
               :weightfn (s/cat :filters ::filters
                                :weightfn ::keyfn
                                :df (s/nilable coll?))))

(defn share
  "Returns the share of values in df for which filter returns true.
  Optionally takes a weightfn to provide weights for each data point.
  Uses the same filter format as where.
  Returns a curried version when only filter is provided."
  ([filters]
   (partial share filters))
  ([filters df]
   (share filters (constantly 1) df))
  ([filters weightfn df]
   (safe-divide (sum weightfn (where filters df))
                (sum weightfn df))))

(s/fdef mean
  :args (s/alt :coll (s/nilable coll?)
               :keyfn (s/cat :keyfn ::keyfn
                             :df (s/nilable coll?))
               :weightfn (s/cat :keyfn ::keyfn
                                :weightfn ::keyfn
                                :df (s/nilable coll?)))
  :ret (s/nilable number?))

(defn mean
  "Calculates the arithmetic mean.
  Optionally takes a keyfn to extract the values and weightfn to provide weights 
  for each data point."
  ([df]
   (mean identity df))
  ([keyfn df]
   (mean keyfn (constantly 1) df))
  ([keyfn weightfn df]
   (let [keyfn (->keyfn keyfn)
         weightfn (->keyfn weightfn)]
     (rate #(* (keyfn %) (weightfn %)) weightfn df))))

(def rollup-mean
  "Rollup and return the mean of aggregations."
  (pcomp mean rollup-vals))

(defn top-n
  "Return n biggest values in coll. 
  Optionally takes a kefyn to extract the values.
  Returns a curried version when only n is provided."
  ([n]
   (partial top-n n))
  ([n df]
   (top-n n identity df))  
  ([n keyfn df]
   (into (empty df) (take n) (sort-by (->keyfn keyfn) > df))))

(s/fdef distribution
  :args (s/alt :coll (s/nilable coll?)
               :keyfn (s/cat :keyfn ::keyfn
                             :df (s/nilable coll?))
               :weightfn (s/cat :keyfn ::keyfn
                                :weightfn ::keyfn
                                :df (s/nilable coll?)))
  :ret (s/and map? sorted?))

(defn distribution
  "Returns a map between all distinct values in df and their relative frequency. 
  Optionally takes a keyfn to extract the values and weightfn to provide weights 
  for each data point."
  ([df]
   (distribution identity df))
  ([keyfn df]
   (distribution keyfn (constantly 1) df))
  ([keyfn weightfn df]
   (when-let [norm (safe-divide (sum weightfn df))]
     (into (priority-map-by >)
       (rollup keyfn (comp (partial * norm) sum) weightfn df)))))

(defn extent
  "Returns a pair of [smallest, biggest] or [earliest, latest] if passed a coll 
  of dates. 
  Optionally takes a keyfn to extract the values."
  ([xs]
   (let [[x & xs] xs]
     (r/fold (r/monoid (if (instance? org.joda.time.DateTime x)
                         (fn [[acc-min acc-max] x]
                           [(t/earliest acc-min x) (t/latest acc-max x)])
                         (fn [[acc-min acc-max] x]
                           [(min acc-min x) (max acc-max x)]))
                       (constantly [x x]))             
             xs)))
  ([keyfn df]
   (extent (col keyfn df))))

(s.test/instrument)
