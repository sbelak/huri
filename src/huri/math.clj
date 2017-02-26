(ns huri.math
  (:require [huri.core :refer [safe-divide distribution]]
            [net.cgrand.xforms :as x]
            [clojure.math.numeric-tower :refer [expt round]]))

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

