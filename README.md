# GCP ETL Data pipeline

·  使用GCP對政府公開資料流進行實時的獲取，並轉換成需要的格式，最後將資料寫入Bigquery或是GCS。

·  應用cloud function, cloud scheduler 去觸發程式執行數據抓取的工作，並將資料透過Pub/Sub進行廣播。

·  透過Apache Beam 設計客製化的Dataflow，將原始資料轉化成目標格式。最終將資料導入Bigquery以方便視覺化。
#
![image](https://github.com/Raydue/GCP_Dataflow_Bus/blob/main/Pubsub_diagram.PNG)

![image](https://github.com/Raydue/GCP_Dataflow_Bus/blob/main/Convert.png)
