(ns huri.time
  (:require [clj-time.core :as t]
            [clojure.math.numeric-tower :refer [ceil]]))

(defn quarter-of-year
  [dt]
  (ceil (/ (t/month dt) 3)))

(defn quarter
  [dt]
  (t/date-time (t/year dt) (inc (* (dec (quarter-of-year dt)) 3))))

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

(defn before-now?
  [dt]
  (t/before? dt (t/now)))

(defn after-now?
  [dt]
  (t/after? dt (t/now)))

(def not-before? (complement before?))
(def not-after? (complement after?))

(defn between?
  [this start end]
  (t/within? (t/interval start end) this))

(defn in?
  ([dt y]
   (= (t/year dt) y))
  ([dt y m]
   (= (year-month dt) (t/date-time y m))))

(defn since?
  [this p]
  (not-before? this (t/minus (if (#{org.joda.time.Years org.joda.time.Months}
                                  (class p))
                               (year-month (t/now))
                               (date (t/now)))
                             p)))
