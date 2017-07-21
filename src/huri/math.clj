(ns huri.math
  (:require [huri.core :refer [safe-divide distribution sum mean]]
            [net.cgrand.xforms :as x]
            [clojure.math.numeric-tower :refer [expt round sqrt abs]]))

(defn smooth
  [window xs]
  (sequence (x/partition window 1 x/avg) xs))

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

(defn clamp
  ([bounds]
   (partial clamp bounds))
  ([[lower upper] x]
   (clamp lower upper x))
  ([lower upper x]
   (max (min x upper) lower)))

(def cdf (comp (partial reductions (fn [[_ acc] [x y]]
                                     [x (+ y acc)]))
               (partial sort-by key)
               distribution))

(defn percentiles
  ([df]
   (percentiles identity df))
  ([keyfn df]
   (percentiles keyfn (constantly 1) df))
  ([keyfn weightfn df]
   (loop [[[k p] & tail] (seq (distribution keyfn weightfn df))
          percentile 1
          acc {}]
     (if k
       (recur tail (- percentile p) (assoc acc k percentile))
       acc))))

(defn kl-divergence
  "Kullback-Leibler divergence of discrete probability distributions `p` and `q`.
   https://en.wikipedia.org/wiki/Kullback%E2%80%93Leibler_divergence"
  [p q]
  (reduce + (map (fn [pi qi]
                   (if (or (zero? pi) (zero? qi))
                     0
                     (* pi (Math/log (/ pi qi)))))
                 p q)))

(defn js-divergence
  "Jensen-Shannon divergence of discrete probability distributions `p` and `q`.
   Note returned is the square root of JS-divergence, so that it obeys the
   metric laws.
   https://en.wikipedia.org/wiki/Jensen%E2%80%93Shannon_divergence"
  [p q]
  (let [m (map (comp (partial * 0.5) +) p q)]
    (sqrt (+ (* 0.5 (kl-divergence p m)) (* 0.5 (kl-divergence q m))))))

(defn euclidean-distance
  "Euclidean distance between vectors `p` and `q`."
  [p q]
  (sqrt (reduce + (map (comp #(* % %) -) p q))))

(defn center
  [p]
  (let [mu (mean p)]
    (map #(- % mu) p)))

(defn em-distance
  [a b]
  (transduce identity
             (fn
               ([]
                {:total-distance 0
                 :last-distance  0})
               ([{:keys [total-distance]}] total-distance)
               ([{:keys [total-distance last-distance]} delta]
                (let [current-distance (+ delta last-distance)]
                  {:total-distance (+ total-distance (abs current-distance))
                   :last-distance current-distance})))
          (map - a b)))
