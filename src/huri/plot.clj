;;;; R interoperability code is based on Jony Hudson's
;;;; https://github.com/JonyEpsilon/gg4clj

(ns huri.plot
  (:require [huri.core :refer [rollup derive-cols sum col extent cols size
                               col-oriented]]
            [clojure.string :as s]            
            [plumbing.core :refer [map-vals for-map assoc-when]]
            [clojure.java.shell :as shell]            
            [clojure.walk :as walk]
            [gorilla-renderable.core :as render]
            [clojure.xml :as xml]
            [clj-time.core :as t])
  (:import org.joda.time.DateTime
           java.io.File
           java.util.UUID))

(declare ->r)

(defn- quote-string  
  [s]
  (str "\"" s "\""))

(defn- function-name  
  [f]
  (case f
    :+ (quote-string "+")
    :<- (quote-string "<-")
    (name f)))

(defn- fn-from-vec
  [[head & tail]]
  (str (function-name head) "(" (s/join ", " (map ->r tail)) ")"))

(defn- named-args-from-map
  [arg-map]
  (->> arg-map
       keys
       (map #(str (name %) " = " (->r (% arg-map))))
       (s/join ", ")))

(defn r+
  [& args]
  (reduce (partial vector :+) args))

(defn ->r
  [code]
  (cond    
    (vector? code) (if (vector? (first code))
                     (s/join ";\n" (map ->r code))
                     (fn-from-vec code))
    (map? code) (named-args-from-map code)
    (keyword? code) (name code)
    (string? code) (quote-string code)
    (or (true? code) (false? code)) (s/upper-case (str code))
    :else (pr-str code)))

(defn- rscript
  [script-path]
  (let [return-val (shell/sh "Rscript" "--vanilla" script-path)]
    (when-not (zero? (:exit return-val))
      (println (:err return-val)))))

(defn- ggsave
  [command filepath width height]
  [command [:ggsave {:filename filepath :width width :height height}]])

(defn- fresh-ids
  [svg]
  (->> svg
       (tree-seq coll? identity)
       (filter map?)
       (keep :id)
       (map (fn [new old]
              {old new
               (str "#" old) (str "#" new)
               (format "url(#%s)" old) (format "url(#%s)" new)})
            (repeatedly #(str (UUID/randomUUID))))
       (apply merge)))

(defn- mangle-ids
  "ggplot produces SVGs with elements that have id attributes. These ids are 
  unique within each plot, but are generated in such a way that they clash when 
  there's more than one plot in a document. 
  This function is a workaround for that. It takes an SVG string and replaces 
  the ids with globally unique ids, returning a string."
  [svg]
  (let [svg (xml/parse (java.io.ByteArrayInputStream. (.getBytes svg)))
        smap (fresh-ids svg)
        mangle (fn [x]
                 (if (map? x)
                   (into {}
                     (for [[k v] x]
                       [k (if (or (= :id k)
                                  (and (string? v)
                                       (or (s/starts-with? v "#")
                                           (s/starts-with? v "url(#"))))
                            (smap v)
                            v)]))
                   x))]
    (with-out-str
      (xml/emit (walk/prewalk mangle svg)))))

(defn render
  ([plot-command]
   (render plot-command {}))
  ([plot-command options]
   (let [width (or (:width options) 6.5)
         height (or (:height options) (/ width 1.618))
         r-file (File/createTempFile "huri-plot" ".r")
         r-path (.getAbsolutePath r-file)         
         out-file (File/createTempFile "huri-plot" ".svg")
         out-path (.getAbsolutePath out-file)
         _ (spit r-path (->r (ggsave plot-command out-path width height)))
         _ (rscript r-path)
         rendered-plot (slurp out-path)
         _ (.delete r-file)
         _ (.delete out-file)]
     (mangle-ids rendered-plot))))

(defrecord GGView [plot-command options])

(extend-type GGView
  render/Renderable
  (render [self]
    {:type :html
     :content (render (:plot-command self) (:options self))
     :value (pr-str self)}))

(defn view
  ([plot-command] (view plot-command {}))
  ([plot-command options]
   (GGView. plot-command options)))

(defn- sanitize-key
  [k]
  (cond
    (nil? k) nil
    (sequential? k) (map sanitize-key k)
    :else (-> (if (or (keyword? k) (symbol? k) (string? k))
                (name k)
                (str k))
              (s/replace #"(?:^\d)|\W" (comp (partial str "__") int first))
              keyword)))

(defmulti ->r-type class)

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

(defmethod ->r-type clojure.lang.BigInt
  [x]
  (long x))

(defmethod ->r-type clojure.lang.Ratio
  [x]
  (double x))

(defmethod ->r-type nil
  [x]
  :NA)

(defmethod ->r-type :default
  [x]
  (str x))

(defmethod ->r-type org.joda.time.DateTime
  [x]
  [:as.Date (str x)])

(defn- ->col-oriented
  [df]
  (cond
    (map? df) (->col-oriented (seq df))
    (map? (first df)) (for-map [k (keys (first df))]
                        (sanitize-key k) (col k df))
    (sequential? (first df)) (->> df
                                  (map (partial zipmap [:x__auto :y__auto]))
                                  ->col-oriented)
    (not-empty df) (->col-oriented (map vector (range) df))))

(defn ->col
  [xs]
  (into [:c] (map ->r-type xs)))

(defn ->matrix
  [df]
  (let [[m n] (size df)]
    [:matrix (->col (apply concat (vals (col-oriented df))))
     {:nrow m
      :ncol n
      :dimnames [:list :NULL (->col (cols df))]}]))

(defn melt
  [cols value-col group-col df]
  (mapcat (fn [col df]
            (for [row df]
              (assoc row value-col (row col)
                     group-col col)))
          cols 
          (repeat df)))

(defn- typespec
  [df]
  (map-vals (comp #(cond
                     (number? %) :number
                     (instance? org.joda.time.DateTime %) :date
                     :else :categorical)
                  first)
            df))

(defn- date-scale-resolution
  [dts]  
  (let [[start end] (extent dts)]
    (if (< (t/in-days (t/interval start end)) 90)
      "%d-%b"
      "%b-%y")))

(def preamble [[:library :ggplot2]
               [:library :scales]
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
  (r+
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
                         :sort-by nil
                         :share-x? false
                         :trendline? false
                         :smoothing-method nil
                         :facet nil
                         :x-rotate nil
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
          (let [{:keys ~(mapv #(symbol (subs (str %) 1)) (keys defaults))
                 :as options#} (merge ~defaults options#)
                total# (when (and ~'trendline? (:stacked? options#))
                         (comp (rollup ~(first positional-params) sum
                                       ~(second positional-params)
                                       df#)
                               ~(first positional-params)))
                used-cols# (->> options#
                                vals                                
                                (concat ~(vec positional-params) [:group__total])
                                flatten
                                (filter keyword?)
                                (map sanitize-key))
                ~'*df* (select-keys (->col-oriented
                                     (if total#
                                       (derive-cols {:group__total total#} df#)
                                       df#))
                                    used-cols#)
                col-types# (typespec ~'*df*)
                ~'x-scale (if (= ~'x-scale :auto)
                            (case (cond->> (col-types# (sanitize-key ~x))
                                    (= '~name '~'bar-chart)
                                    (#(get #{:date} % :categorical)))
                              :date :dates
                              :categorical :categorical
                              :linear)
                            ~'x-scale)
                ~'y-scale (if (= ~'y-scale :auto)
                            (case (col-types# (sanitize-key ~y))
                              :date :dates
                              :categorical :categorical
                              :linear)
                            ~'y-scale)
                ~'x-label (or ~'x-label
                              (if (#{:x__auto :y__auto} ~x)
                                ""
                                (name ~x))) 
                ~'y-label (or ~'y-label
                              ~(if y
                                 `(if (not= ~y :y__auto)
                                    (name ~y)
                                    "")
                                 ""))]            
            (view 
             [[:<- :g [:data.frame (map-vals ->col ~'*df*)]]
              preamble
              (->> (let [~@(mapcat #(vector % `(sanitize-key ~%))
                                   (concat positional-params
                                           ['group-by 'facet 'sort-by]))]
                     (concat ~body                              
                             [(when ~'facet
                                [:facet_grid (keyword
                                              (if (sequential? ~'facet)
                                                (->> ~'facet
                                                     (map name)
                                                     (s/join " ~ "))
                                                (str "~" (name ~'facet))))])
                              (when ~'trendline?
                                [:geom_smooth
                                 [:aes (cond
                                         total# {:y :group__total}
                                         ~'group-by {:group ~'group-by}
                                         :else {})]
                                 (merge {:alpha 0.25
                                         :colour "black"
                                         :fill "black"}
                                        (when ~'smoothing-method
                                          {:method (name ~'smoothing-method)}))])
                              (when (and ~'share-x? ~'group-by)
                                [:facet_grid (->> ~'group-by
                                                  name
                                                  (format "%s ~ .")
                                                  keyword)
                                 {:scales "free_y"}])
                              (case ~'x-scale
                                :log [:scale_x_log10 {:labels :comma}]
                                :sqrt [:scale_x_sqrt {:labels :comma}]
                                :linear [:scale_x_continuous {:labels :comma}]
                                :percent [:scale_x_continuous {:labels :percent}]
                                :dates [:scale_x_date
                                        {:labels [:date_format
                                                  (date-scale-resolution
                                                   (~'*df* ~x))]}]
                                :categorical nil) 
                              (case ~'y-scale
                                :log [:scale_y_log10 {:labels :comma}]
                                :sqrt [:scale_y_sqrt {:labels :comma}]
                                :linear [:scale_y_continuous {:labels :comma}]
                                :percent [:scale_y_continuous {:labels :percent}]
                                :dates [:scale_y_date
                                        {:labels [:date_format
                                                  (date-scale-resolution
                                                   (~'*df* ~y))]}]
                                :categorical nil)
                              theme 
                              (when-not (or (true? ~'legend?)
                                            (and ~'legend?
                                                 ~'group-by
                                                 (not ~'share-x?)
                                                 (not ~'facet)))
                                [:theme {:legend.position "none"}])
                              (when (or (number? ~'x-rotate) 
                                        (and (= ~'x-rotate :auto)
                                             (nil? (:flip? options#))))
                                [:theme
                                 {:axis.text.x [:element_text
                                                {:angle (if (number? ~'x-rotate)
                                                          ~'x-rotate
                                                          45)
                                                 :hjust 1}]}])
                              [:labs {:x ~'x-label
                                      :y ~'y-label
                                      :title ~'title}]]))
                   (remove nil?)
                   (apply r+))]
             {:width ~'width :height ~'height})))))))

(defn format-value
  [x percent?]
  (if percent?
    [:sprintf "%1.2f%%" (->> x
                             name
                             (format "100*%s")
                             keyword)]
    x))

(defplot histogram x {:bins 20 
                      :bin-width nil 
                      :density? false
                      :frequency? false
                      :show-mean? true}
  (let [bin-width (or bin-width
                      (/ (- (apply max (*df* x)) (apply min (*df* x)))
                         bins))
        aesthetics (if frequency?
                     {:colour (or group-by colour)}
                     {:fill (or group-by colour)})]
    [[:ggplot :g (if density? 
                   [:aes x (keyword "..density..") (if group-by
                                                     aesthetics
                                                     {})]
                   [:aes x (if group-by
                             aesthetics
                             {})])]
     [(if frequency?
        :geom_freqpoly
        :geom_histogram) (merge {:binwidth bin-width 
                                 :alpha alpha}
                                (when-not group-by
                                  aesthetics))]
     (when show-mean?
       [:geom_vline [:aes {:xintercept [:mean x]}] {:linetype "dashed"
                                                    :color (or group-by colour)
                                                    :size 0.5}])
     [:geom_hline {:yintercept 0 :size 0.4 :colour "black"}]]))

(defplot line-chart x y {:show-points? :auto
                         :fill? false
                         :alpha 0.5
                         :size nil
                         :show-labels? false}
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
     (let [aesthetics (merge {:alpha (if size
                                       alpha
                                       1)}
                             (when-not group-by
                               {:colour colour}))]
       (if size
         [:geom_point [:aes {:size size}] aesthetics]
         [:geom_point aesthetics])))
   (when show-labels?
     [:geom_label_repel
      [:aes (-> {:label (format-value y (= y-scale :percent))}
                (assoc-when :fill (some->> group-by (vector :factor))))]
      {:size 3.5
       :color (if group-by "white" "black")
       :show.legend false}])
   (when fill?
     [:geom_area (merge {:alpha alpha}
                        (when-not group-by
                          {:fill colour}))])])

(defplot bar-chart x y {:stacked? false 
                        :flip? false
                        :sort-by nil
                        :show-values? false
                        :x-rotate :auto} 
  [[:ggplot :g [:aes (-> {:x (if (= :dates x-scale)
                               x
                               [:reorder x (or sort-by y)])
                          :y y}
                         (assoc-when :fill group-by))]] 
   [:geom_bar (merge {:stat "identity"} 
                     (if group-by
                       (when-not stacked? 
                         {:position "dodge"})
                       {:fill colour}))]
   (when show-values?
     [:geom_text [:aes {:label (format-value y (= y-scale :percent))}]
      {:size 2.5
       :color (if (and stacked? (not flip?)) "white" "black")
       :hjust (if flip? 0 0.5)
       :vjust (cond
                flip? 0.5
                stacked? 1.8
                :else -0.3)
       :position (if (and stacked? (not flip?)) "stack" [:position_dodge 1])}])
   (when (and stacked? show-values?) 
     [:geom_text [:aes {:label (format-value :group__total (= y-scale :percent))
                        :y :group__total}]
        {:size 2.5
         :hjust (if flip? 0 0.5)
         :vjust (if flip? 0.5 -0.3)}])
   (when flip?
     [:coord_flip])])

(defplot scatter-plot x y {:alpha 0.5
                           :label nil
                           :size nil}
  [[:ggplot :g [:aes (-> {:x x :y y}
                         (assoc-when :colour group-by)
                         )]]
   [:geom_point [:aes (if size
                        {:size size}
                        {})]
    (merge {:alpha alpha}
           (when-not group-by
             {:colour colour}))]
   (when label
     [:geom_label_repel
      [:aes (-> {:label label}
                (assoc-when :color (some->> group-by (vector :factor))))]
      {:size 3.5
       :show.legend false}])])

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
                  :trim (boolean trim?)
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
        (assoc-when :limit (some->> extent ->col)))]])
