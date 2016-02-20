(ns huri.plot
  (:require [gg4clj.core :as gg4clj]
            [clojure.string :as s]
            [plumbing.core :refer [assoc-when]]
            [plumbing.core :refer [map-vals for-map]])
  (:import org.joda.time.DateTime))

(def preamble [[:library :scales]
               [:library :grid]
               [:library :RColorBrewer]
               [:library :ggrepel]
               [:<- :palette [:brewer.pal "Greys" {:n 9}]]
               {:color.background (keyword "palette[2]")}
               {:color.grid.major (keyword "palette[3]")}
               {:color.axis.text (keyword "palette[6]")}
               {:color.axis.title (keyword "palette[7]")}
               {:color.title (keyword "palette[9]")}])

(def theme
  (gg4clj/r+
    [:theme_bw {:base_size 9}]
    [:theme {:panel.background [:element_rect {:fill :color.background 
                                               :color :color.background}]}]
    [:theme {:plot.background [:element_rect {:fill :color.background 
                                              :color :color.background}]}]
    [:theme {:panel.border [:element_rect {:color :color.background}]}]
    [:theme {:panel.grid.major [:element_line {:color :color.grid.major 
                                               :size 0.25}]}]
    [:theme {:panel.grid.minor [:element_blank]}]
    [:theme {:axis.ticks [:element_blank]}]
    [:theme {:legend.background [:element_rect {:fill :color.background}]}]
    [:theme {:legend.key [:element_rect {:fill :color.background 
                                         :color :color.background}]}]
    [:theme {:legend.text [:element_text {:size 7
                                          :color :color.axis.title}]}]
    [:theme {:legend.title [:element_blank]}]
    [:theme {:plot.title [:element_text {:size 10
                                         :color :color.title 
                                         :vjust 1.25}]}]
    [:theme {:axis.text.x [:element_text {:size 7
                                          :color :color.axis.text}]}]
    [:theme {:axis.text.y [:element_text {:size 7
                                          :color :color.axis.text}]}]
    [:theme {:axis.title.x [:element_text {:size 8 
                                           :color :color.axis.title 
                                           :vjust 0}]}]
    [:theme {:axis.title.y [:element_text {:size 8 
                                           :color :color.axis.title 
                                           :vjust 1.25}]}]
    [:theme {:plot.margin [:unit [:c 0.35 0.2 0.3 0.35] "cm"]}]))

(defn sanitize-key
  [k]
  (when k
    (-> (if (or (keyword? k) (symbol? k) (string? k))
          (name k)
          (str k))
        (s/replace #"(?:^\d)|\W" #(str "__" (int (first %))))
        keyword)))

(defmulti ->r-type type)

(defmethod ->r-type clojure.lang.Keyword
  [x]
  (name x))

(defmethod ->r-type java.lang.String
  [x]
  x)

(defmethod ->r-type java.lang.Long
  [x]
  x)

(defmethod ->r-type java.lang.Double
  [x]
  x)

(defmethod ->r-type java.lang.Integer
  [x]
  x)

(defmethod ->r-type java.lang.Float
  [x]
  x)

(defmethod ->r-type clojure.lang.Ratio
  [x]
  (float x))

(defmethod ->r-type nil
  [x]
  :NA)

(defmethod ->r-type :default
  [x]
  (str x))

(defmethod ->r-type org.joda.time.DateTime
  [x]
  [:as.Date (str x)])

(defn ->r-data-frame
  [df]
  (cond
    (map? df) (->r-data-frame (seq df))
    (map? (first df)) (for-map [k (keys (first df))]
                               (sanitize-key k) (map (comp ->r-type k) df))
    (sequential? (first df)) (->> df
                                  (map (partial zipmap [:x__auto :y__auto]))
                                  ->r-data-frame)
    :else (->r-data-frame (map vector (range) df))))

(defn melt
  [cols value-col group-col df]
  (mapcat (fn [col df]
            (for [row df]
              (assoc row value-col (row col)
                group-col col)))
          cols 
          (repeat df)))

(defn typespec
  [df]
  (map-vals (comp #(cond
                     (number? %) :number
                     (string? %) :categorical
                     (and (vector? %) (= :as.Date (first %))) :date)
                  first)
            df))

(defmacro defplot
  [name & args]
  (let [params (butlast args) 
        positional-params (butlast params)
        [x y] positional-params
        defaults (merge {:x-label nil
                         :y-label nil
                         :title ""
                         :x-scale :auto
                         :y-scale :auto
                         :group-by nil
                         :colour "#c0392b"
                         :alpha 0.75
                         :legend? :auto
                         :share-x? false
                         :trendline? false                         
                         :facet nil
                         :width 9
                         :height 5}
                        (last params))
        body (last args)]
    `(defn ~name
       ([df#]
        (~name ~@(if y
                   [:x__auto :y__auto]
                   [:y__auto]) {} df#))
       ~@(if y
           [`([options# df#]
              (~name :x__auto :y__auto options# df#))
            `([~@positional-params df#]
              (~name ~@positional-params {} df#))]
           [`([arg# df#]
              (if (map? arg#)
                (~name :y__auto arg# df#)
                (~name arg# {} df#)))])       
       ([~@positional-params options# df#]
        (if (sequential? ~(last positional-params))
          (~name ~@(butlast positional-params) :y__auto 
                 (assoc options# :group-by :series__auto) 
                 (melt ~(last positional-params) :y__auto :series__auto df#))
          (let [{:keys ~(mapv #(symbol (subs (str %) 1)) (keys defaults))} 
                (merge ~defaults options#)
                ~'*df* (->r-data-frame df#)
                col-types# (typespec ~'*df*)
                ~'x-scale (if (= ~'x-scale :auto)
                            (case (col-types# ~x)
                              :date :dates
                              :categorical :categorical
                              :linear)
                            ~'x-scale)
                ~'y-scale (if (= ~'y-scale :auto)
                            (case (col-types# ~y)
                              :date :dates
                              :categorical :categorical
                              :linear)
                            ~'y-scale)]
            (gg4clj/view 
              [[:<- :g (gg4clj/data-frame ~'*df*)]
               preamble
               (->> [(case ~'x-scale
                       :log [:scale_x_log10 {:labels :comma}]
                       :sqrt [:scale_x_sqrt {:labels :comma}]
                       :linear [:scale_x_continuous {:labels :comma}]
                       :percent [:scale_x_continuous {:labels :percent}]
                       :months [:scale_x_date {:labels [:date_format "%b-%y"]}]
                       :dates [:scale_x_date {:labels [:date_format "%d-%b"]}]
                       :categorical nil) 
                     (case ~'y-scale
                       :log [:scale_y_log10 {:labels :comma}]
                       :sqrt [:scale_y_sqrt {:labels :comma}]
                       :linear [:scale_y_continuous {:labels :comma}]
                       :percent [:scale_y_continuous {:labels :percent}]
                       :months [:scale_y_date {:labels [:date_format "%b-%y"]}]
                       :dates [:scale_y_date {:labels [:date_format "%d-%b"]}]
                       :categorical nil)
                     theme 
                     (when-not (or (true? ~'legend?)
                                   (and ~'legend?
                                         ~'group-by
                                         (not ~'share-x?)
                                         (not ~'facet)))
                       [:theme {:legend.position "none"}])
                     [:labs {:x (or ~'x-label
                                    (if (#{:x__auto :y__auto} ~x)
                                      ""
                                      (name ~x))) 
                             :y (or ~'y-label
                                    ~(if y
                                       `(if (not= ~y :y__auto)
                                          (name ~y)
                                          "")
                                       ""))
                             :title ~'title}]]
                    (concat (let [~@(mapcat #(vector % `(sanitize-key ~%))
                                            (conj positional-params 'group-by))] 
                              ~body)
                            [(when ~'facet
                               [:facet_grid (keyword
                                             (if (sequential? ~'facet)
                                               (->> ~'facet
                                                    (map name)
                                                    (apply format "%s ~ %s"))
                                               (str "~" (name ~'facet))))])
                             (when ~'trendline?
                               [:geom_smooth {:alpha 0.25
                                              :colour "black"
                                              :fill "black"}])])
                    (remove nil?)
                    (apply gg4clj/r+))]
              {:width ~'width :height ~'height})))))))

(defplot histogram x {:bins 20 
                      :bin-width nil 
                      :density? false}
  (let [bin-width (or bin-width
                      (/ (- (apply max (*df* x)) (apply min (*df* x)))
                         bins))
        aesthetics (merge {:binwidth bin-width :alpha alpha}
                          (if group-by
                            {:position "identity"}
                            {:fill colour}))]
    [[:ggplot :g [:aes (-> {:x x}
                           (assoc-when :fill group-by))]]
     (if density?
       [:geom_histogram [:aes {:y (keyword "..density..")}] 
        aesthetics]
       [:geom_histogram aesthetics])
     [:geom_hline {:yintercept 0 :size 0.4 :colour "black"}]
     (when density?
       [:geom_density])]))

(defplot line-chart x y {:show-points? :auto
                         :fill? false
                         :alpha 0.5
                         :share-x? false}
  [[:ggplot :g [:aes (merge {:x x :y y} 
                            (when group-by
                              {:group group-by 
                               :colour group-by
                               :fill group-by}))]] 
   [:geom_line (if group-by 
                 {}
                 {:colour colour})]
   (when (or (true? show-points?) 
             (and (= show-points? :auto)
                  (not fill?)
                  (< (count (*df* x)) 50)))
     [:geom_point (if group-by
                    {}
                    {:colour colour})])
   (when fill?
     [:geom_area (merge {:alpha alpha}
                        (when-not group-by
                          {:fill colour}))])
   (when (and share-x? group-by)
     [:facet_grid (keyword (format "%s ~ ." (name group-by)))
      {:scales "free_y"}])])

(defplot bar-chart x y {:stacked? false 
                        :flip? false} 
  [[:ggplot :g [:aes (merge {:x x :y y}
                            (when group-by
                              {:fill group-by}))]] 
   [:geom_bar (merge {:stat "identity"} 
                     (if group-by
                       (when-not stacked? 
                         {:position "dodge"})
                       {:fill colour}))]
   (when flip?
     [:coord_flip])])

(defplot scatter-plot x y {:alpha 0.5
                           :label nil}
  [[:ggplot :g [:aes (-> {:x x :y y}
                         (assoc-when :colour group-by))]]
   [:geom_point (merge {:alpha alpha}
                       (when-not group-by
                         {:colour colour}))]
   (when label
     [:geom_label_repel [:aes (-> {:label label}
                                  (assoc-when :color (some->> group-by
                                                              (vector :factor))))]

      {:size 3.5
       :show.legend :FALSE}])])

(defplot box-plot x y {:legend? false}
  [[:ggplot :g [:aes {:x x :y y :fill x}]]
   [:geom_boxplot]])

(defplot violin-plot x y {:legend? false
                          :trim? true
                          :scale :count
                          :summary? true}
  [[:ggplot :g [:aes {:x x :y y :fill x}]]
   [:geom_violin {:alpha 0.5
                  :colour (keyword "palette[4]")
                  :trim (if trim? :TRUE :FALSE)
                  :scale (name scale)}]
   (when summary?
     [:stat_summary {:fun.data "mean_se" 
                     :geom "pointrange"}])])

(defplot heatmap x y z {:extent nil
                        :z-label nil
                        :legend? true
                        :legend-title true}
  [[:ggplot :g [:aes x y {:fill z}]]
   [:geom_tile]
   [:scale_fill_distiller (or z-label (name z))
    (-> {:palette "RdYlBu"}
        (assoc-when :limit (some->> extent (apply vector :c))))]])

