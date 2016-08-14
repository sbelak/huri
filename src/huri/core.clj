(ns huri.core
  (:require (plumbing [core :refer [distinct-fast map-vals safe-get for-map
                                    map-from-vals]]
                      [map :refer [safe-select-keys]])
            [net.cgrand.xforms :as x]
            [clojure.data.priority-map :refer [priority-map-by]]
            [clojure.math.numeric-tower :refer [expt round]]
            [clj-time.core :as t]
            [clojure.core.reducers :as r]
            [clojure.spec :as s]
            [clojure.spec.test :as s.test])
  (:import org.joda.time.DateTime))

(defn papply
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
  ([f]
   (map (papply f)))
  ([f coll & colls]
   (apply sequence (mapply f) coll colls)))

(def mapm (comp (partial into {}) map))

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
  [& keyvals]
  `(apply hash-map (pvalues ~@keyvals)))

(defn fsome
  [f]
  (fn [& args]
    (when (every? some? args)
      (apply f args))))

(def transpose (papply map vector))

(defn val-or-seq
  [element-type]
  (s/and
   (s/or :seq (s/coll-of element-type)
         :val element-type)
   (with-conformer x
     :seq x
     :val [x])))

(def ensure-seq (partial s/conform (val-or-seq any?)))

(s/def ::keyfn (s/and
                (s/or :kw keyword?
                      :fn ifn?)
                (with-conformer x
                  :kw #(safe-get % x)
                  :fn x)))

(def ->keyfn (partial s/conform ::keyfn))

(s/def ::dataframe (s/nilable (s/every map?)))

(defn col
  ([k]
   (map (->keyfn k)))
  ([k df]
   (sequence (col k) df)))

(defn any-of
  [& keyfns]
  {::combinator some-fn
   ::keyfns keyfns})

(defn every-of
  [& keyfns]
  {::combinator every-pred
   ::keyfns keyfns})

(s/def ::combinator fn?)

(s/def ::keyfns (s/+ ::keyfn))

(s/def ::key-combinator (s/and
                         (s/or :combinator (s/keys :req [::combinator ::keyfns])
                               :keyfn ::keyfn)
                         (with-conformer x
                           :combinator x
                           :keyfn {::combinator identity
                                   ::keyfns [x]})))

(s/def ::pred (s/and
               (s/or :vec (s/and vector? (s/cat :f ifn? :args (s/* any?)))
                     :fn ifn?
                     :val (complement ifn?))
               (with-conformer x
                 :vec #(apply (:f x) % (:args x))
                 :fn x
                 :val (partial = x))))

(s/def ::filters (s/or :map (s/map-of ::key-combinator ::pred :conform-keys true)
                       :pred ifn?))

(s/fdef where
  :args (s/alt :curried (s/cat :filters ::filters)
               :full (s/cat :filters ::filters
                            :df (s/nilable coll?)))
  :ret coll?)

(defn where
  ([filters]
   (partial where filters))
  ([filters df]
   (into (empty df)
     (filter
      (let [[tag filters] (s/conform ::filters filters)]
        (if (= tag :map)
          (->> filters
               (map (fn [[{:keys [::combinator ::keyfns]} pred]]
                      (apply combinator (map (partial comp pred) keyfns))))
               (apply every-pred))
          filters)))
     df)))

(s/def ::summary-fn
  (s/or :map (s/map-of keyword? (s/and
                                 (s/or :vec (s/cat :f ifn?
                                                   :keyfn (val-or-seq ::keyfn)
                                                   :filters (s/? ::filters))
                                       :fn fn?)
                                 (with-conformer x
                                   :vec x
                                   :fn {:f x :keyfn identity})))
        :fn fn?))

(s/fdef summary
  :args (s/alt :curried (s/cat :f ::summary-fn)
               :simple (s/cat :f ::summary-fn
                              :df (s/nilable coll?))
               :keyfn (s/cat :f ::summary-fn
                             :keyfn (val-or-seq ::keyfn)
                             :df (s/nilable coll?)))
  :ret any?)

(defn summary
  ([f]
   (partial summary f))
  ([f df]
   (summary f identity df))
  ([f keyfn df]
   (let [[tag f] (s/conform ::summary-fn f)]
     (if (= tag :map)
       (map-vals (fn [{f :f keyfn-local :keyfn [_ filters] :filters}]
                   (summary f ({identity keyfn-local} keyfn) 
                            (cond->> df filters (where filters))))
                 f)
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
                                       (map (partial summary f keyfn))))       	
     df)))

(def rollup-vals (pcomp vals rollup))
(def rollup-keep (pcomp (partial remove nil?) rollup-vals))
(def rollup-cat (pcomp (papply concat) rollup-vals))

(s/def ::fuse-fn (s/and (s/or :map map?
                              :kw keyword?
                              :fn ifn?)
                        (with-conformer x
                          :map x
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
                    (merge (into {} (summary f keyfn group))
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
                   (map-vals (constantly (sorted-map)) f)))))

(s/fdef window
  :args (s/alt :simple (s/cat :f ifn?
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
  ([f df]
   (window f identity df))
  ([f keyfn df]
   (window 1 f keyfn df))
  ([lag f keyfn df]
   (let [xs (col keyfn df)]
     (map f (drop lag xs) xs))))

(s/fdef size
  :args (s/every coll?)
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

(s/def ::col-transforms
  (s/map-of (val-or-seq keyword?)
            (s/or :vec (s/and vector? (s/cat :f ifn? :keyfns (s/+ ::keyfn)))
                  :ifn ifn?)
            :conform-keys true))

(s/fdef derive-cols
  :args (s/cat :new-cols ::col-transforms
               :df ::dataframe)
  :ret ::dataframe)

(defn derive-cols
  [new-cols df]
  (map (->> new-cols
            (s/conform ::col-transforms)
            (map (fn [[ks [tag f]]]
                   (let [f (if (= tag :vec)
                             (let [{:keys [f keyfns]} f]
                               (comp (papply f) (apply juxt keyfns)))
                             f)]
                     (fn [row]
                       (assoc-in row ks (f row))))))
            (apply comp))
       df))

(defn update-cols
  [update-fns df]
  (derive-cols (for-map [[k f] update-fns]
                 k [f k])
               df))

(defn ->data-frame
  [cols xs]
  (if (and (not= (count cols) (count (first xs)))
           (some coll? (first xs)))    
    (->data-frame cols (map (partial mapcat ensure-seq) xs))
    (map (partial zipmap cols) xs)))

(defn select-cols
  [cols df]
  (map #(safe-select-keys % cols) df))

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
      (for [row left
            :when (or (left->right row) (= op :left-join))]
        (merge row (left->right row)))))))

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
  :args (s/alt :coll (s/cat :df (s/nilable coll?))
               :keyfn (s/cat :keyfn ::keyfn
                             :df (s/nilable coll?)))
  :ret number?)

(defn sum
  ([df]
   (sum identity df))
  ([keyfn df]
   (transduce (col keyfn) + df)))

(defn rate
  ([keyfn-a keyfn-b]
   (partial rate keyfn-a keyfn-b))
  ([keyfn-a keyfn-b df]
   (safe-divide (sum keyfn-a df)
                (sum keyfn-b df))))

(s/fdef share
  :args (s/alt :curried (s/cat :filters ::filters)
               :simple (s/cat :filters ::filters
                              :df (s/nilable coll?))
               :weightfn (s/cat :filters ::filters
                                :weightfn ::keyfn
                                :df (s/nilable coll?))))

(defn share
  ([filters]
   (partial share filters))
  ([filters df]
   (safe-divide (count-where filters df) (count df)))
  ([filters weightfn df]   
   (safe-divide (sum weightfn (where filters df))
                (sum weightfn df))))

(defn top-n
  ([n df]
   (top-n n identity df))  
  ([n keyfn df]
   (->> df
        (sort-by (->keyfn keyfn) >)
        (take n)
        (into (empty df)))))

(s/fdef distribution
  :args (s/alt :coll (s/cat :df (s/nilable coll?))
               :keyfn (s/cat :keyfn ::keyfn
                             :df (s/nilable coll?))
               :weightfn (s/cat :keyfn ::keyfn
                                :weightfn ::keyfn
                                :df (s/nilable coll?)))
  :ret (s/and map? sorted?))

(defn distribution
  ([df]
   (distribution identity df))
  ([keyfn df]
   (distribution keyfn (constantly 1) df))
  ([keyfn weightfn df]
   (when-let [norm (safe-divide (sum weightfn df))]
     (into (priority-map-by >)
       (rollup keyfn (comp (partial * norm) sum) weightfn df)))))

(def cdf (comp (partial reductions (fn [[_ acc] [x y]]
                                     [x (+ y acc)]))
               distribution))

(defn percentiles
  ([df]
   (percentiles identity df))
  ([keyfn df]
   (percentiles keyfn (constantly 1) df))
  ([keyfn weightfn df]
   (loop [[[k p] & tail] (seq (distribution keyfn weightfn df))
          percentile 1
          acc []]
     (if k
       (recur tail (- percentile p) (conj acc [k percentile]))
       (into {} acc)))))

(s/fdef mean
  :args (s/alt :coll (s/cat :df (s/nilable coll?))
               :keyfn (s/cat :keyfn ::keyfn
                             :df (s/nilable coll?))
               :weightfn (s/cat :keyfn ::keyfn
                                :weightfn ::keyfn
                                :df (s/nilable coll?)))
  :ret (s/nilable number?))

(defn mean
  ([df]
   (mean identity df))
  ([keyfn df]
   (some->> df not-empty (transduce (col keyfn) x/avg) double))
  ([keyfn weightfn df]
   (let [keyfn (->keyfn keyfn)
         weightfn (->keyfn weightfn)]
     (rate #(* (keyfn %) (weightfn %)) weightfn df))))

(defn harmonic-mean
  ([df]
   (harmonic-mean identity df))
  ([keyfn df]
   (double (/ (count df) (sum (comp / keyfn) df)))))

(defn smooth
  [window xs]
  (sequence (x/partition window 1 (x/reduce x/avg)) xs))

(defn growth
  [b a]
  (safe-divide (* (if (neg? a) -1 1) (- b a)) a)) 

(defn decay
  [lambda t]
  (expt Math/E (- (* lambda t))))

(defn logistic
  [L k x0 x]
  (/ L (+ 1 (decay k (- x x0)))))

(def entropy (comp -
                   (partial sum #(* % (Math/log %)))
                   vals
                   distribution))

(defn round-to
  ([precision]
   (partial round-to precision))
  ([precision x]
   (let [scale (/ precision)]
     (/ (round (* x scale)) scale))))

(defn extent
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

(defn clamp
  ([bounds]
   (partial clamp bounds))
  ([[lower upper] x]
   (clamp lower upper x))
  ([lower upper x]
   (max (min x upper) lower)))

(defn nil->0
  [x]
  (or x 0))

(s.test/instrument)
