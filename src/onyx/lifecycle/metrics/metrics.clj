(ns onyx.lifecycle.metrics.metrics
  (:require [taoensso.timbre :refer [info warn fatal]]
            [metrics.core :refer [new-registry]]
            [clojure.set :refer [rename-keys]]
            [onyx.static.util :refer [kw->fn]]
            [onyx.protocol.task-state :as task]
            [metrics.counters :as c]
            [metrics.timers :as t]
            [metrics.gauges :as g]
            [metrics.meters :as m])
  (:import [java.util.concurrent TimeUnit]
           [com.codahale.metrics JmxReporter]))

(defn new-lifecycle-latency [reg job-name task-name id lifecycle]
  (let [timer ^com.codahale.metrics.Timer (t/timer reg ["job" job-name "task" task-name "peer-id" 
                                                        (str id) "task-lifecycle" 
                                                        (name lifecycle)])] 
    (fn [state latency-ns]
      (.update timer latency-ns TimeUnit/NANOSECONDS))))

(defn new-read-batch [reg job-name task-name id lifecycle]
  (let [throughput (m/meter reg ["job" job-name "task" task-name (name lifecycle) "throughput"])
        timer ^com.codahale.metrics.Timer (t/timer reg ["job" job-name "task" task-name "peer-id" 
                                                        (str id) "task-lifecycle" 
                                                        (name lifecycle)])] 
    (fn [state latency-ns]
      (m/mark! throughput (count (:onyx.core/batch (task/get-event state))))
      (.update timer latency-ns TimeUnit/NANOSECONDS))))

(defn new-write-batch [reg job-name task-name id lifecycle]
  (let [throughput (m/meter reg ["job" job-name "task" task-name "peer-id" 
                                 (str id) "task-lifecycle" (name lifecycle) "throughput"])
        timer ^com.codahale.metrics.Timer 
        (t/timer reg ["job" job-name "task" task-name "peer-id" 
                      (str id) "task-lifecycle" (name lifecycle)])] 
    (fn [state latency-ns]
      ;; TODO, for blockable lifecycles, keep adding latencies until advance?
      (.update timer latency-ns TimeUnit/NANOSECONDS)
      (when (task/advanced? state)
        (m/mark! throughput (reduce (fn [c {:keys [leaves]}]
                                      (+ c (count leaves)))
                                    0
                                    (:tree (:onyx.core/results (task/get-event state)))))))))

(defn update-rv-epoch [cnt-replica-version cnt-epoch epoch-rate]
  (fn [state latency-ns]
    (m/mark! epoch-rate 1)
    (c/inc! cnt-replica-version (- (task/replica-version state) 
                                   (c/value cnt-replica-version)))
    (c/inc! cnt-epoch (- (task/epoch state) (c/value cnt-epoch)))))

;; FIXME, close counters and remove them from the registry
(defn before-task [{:keys [onyx.core/job-id onyx.core/id 
                           onyx.core/monitoring onyx.core/task] :as event} 
                   lifecycle] 
  (when (:metrics/workflow-name lifecycle)
    (throw (ex-info ":metrics/workflow-name has been deprecated. Use job metadata such as:
                     {:workflow ...
                      :lifecycles ...
                      :metadata {:name \"YOURJOBNAME\"}}
                      to supply your job's name" {})))
  (let [lifecycles (or (:metrics/lifecycles lifecycle) 
                       #{:lifecycle/read-batch :lifecycle/write-batch 
                         :lifecycle/apply-fn :lifecycle/unblock-subscribers})
        job-name (str (get-in event [:onyx.core/task-information :metadata :name] job-id))
        task-name (name (:onyx.core/task event))
        task-registry (new-registry)
        cnt-replica-version (c/counter task-registry ["job" job-name "task" task-name "peer-id" (str id) "replica-version"])
        cnt-epoch (c/counter task-registry ["job" job-name "task" task-name "peer-id" (str id) "epoch"])
        epoch-rate (m/meter task-registry ["job" job-name "task" task-name "peer-id" (str id) "epoch-rate"])
        update-rv-epoch-fn (update-rv-epoch cnt-replica-version cnt-epoch epoch-rate)
        reporter (.build (JmxReporter/forRegistry task-registry))
        _ (.start ^JmxReporter reporter)]
    {:onyx.core/monitoring (assoc (reduce 
                                   (fn [mon lifecycle]
                                     (assoc mon 
                                            lifecycle 
                                            (case lifecycle
                                              :lifecycle/unblock-subscribers update-rv-epoch-fn
                                              :lifecycle/read-batch (new-read-batch task-registry job-name task-name id :lifecycle/read-batch) 
                                              :lifecycle/write-batch (new-write-batch task-registry job-name task-name id :lifecycle/write-batch) 
                                              (new-lifecycle-latency task-registry job-name task-name id lifecycle))))
                                   monitoring
                                   lifecycles)
                                  :task-registry task-registry
                                  :reporter reporter)}))

(defn after-task [{:keys [onyx.core/monitoring]} lifecycle]
  (-> monitoring :task-registry metrics.core/remove-all-metrics) 
  (.stop ^JmxReporter (:reporter monitoring)) 
  {})

(def calls
  {:lifecycle/after-task-stop after-task
   :lifecycle/before-task-start before-task})
