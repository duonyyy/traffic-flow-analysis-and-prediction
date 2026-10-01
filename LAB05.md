



LAB 05: HỆ THỐNG PHÂN TÍCH VÀ DỰ ĐOÁN LƯU LƯỢNG GIAO THÔNG ĐÔ THỊ













## CHƯƠNG 2: CƠ SỞ LÝ THUYẾT VÀ NỀN TẢNG CÔNG NGHỆ



### 2.1. Kiến trúc tổng thể: Lambda Architecture

Đồ án được thiết kế dựa trên các nguyên lý của Lambda Architecture, một mô hình thiết kế phần mềm phổ biến trong xử lý dữ liệu lớn nhằm cân bằng giữa độ trễ (latency) và độ chính xác (accuracy).14

Kiến trúc Lambda bao gồm 3 lớp chính:

Batch Layer (Lớp xử lý lô): Chịu trách nhiệm quản lý tập dữ liệu gốc (master dataset) bất biến và tính toán các view (góc nhìn) dữ liệu lịch sử. Trong đồ án này, Hadoop HDFS đóng vai trò lưu trữ master dataset, và Apache Spark thực hiện việc tính toán mô hình dự báo (Machine Learning) trên toàn bộ dữ liệu lịch sử. Ưu điểm là độ chính xác cao nhưng độ trễ lớn.16

Speed Layer (Lớp xử lý tốc độ): Xử lý dữ liệu mới sinh ra để bù đắp cho độ trễ của Batch Layer. Mặc dù trong phạm vi đồ án hiện tại, phần xử lý thời gian thực được tích hợp nhẹ thông qua việc gọi API trực tiếp tại thời điểm hiển thị, nhưng kiến trúc tổng thể đã sẵn sàng cho việc tích hợp Spark Streaming hoặc Kafka trong tương lai.17

Serving Layer (Lớp phục vụ): Hợp nhất kết quả từ Batch và Speed layer để trả lời các truy vấn của người dùng. Streamlit đóng vai trò là giao diện người dùng, truy vấn mô hình đã được huấn luyện từ Batch Layer để hiển thị dự báo.18

### 2.2. Hệ sinh thái Apache Hadoop và HDFS

Hadoop Distributed File System (HDFS) là xương sống của lớp lưu trữ. Đối với dữ liệu giao thông thu thập liên tục theo chuỗi thời gian (time-series), HDFS cung cấp khả năng mở rộng vượt trội so với ổ cứng cục bộ.

Cơ chế hoạt động: HDFS chia các tệp tin lớn (ví dụ: các file CSV log giao thông tích lũy theo tháng) thành các khối (blocks), mặc định là 128MB. Các khối này được sao chép (replicate) ra nhiều máy trạm (DataNodes) khác nhau (thường là 3 bản sao). Điều này đảm bảo rằng nếu một máy chủ gặp sự cố phần cứng, dữ liệu giao thông không bị mất và hệ thống vẫn có thể phục vụ truy vấn.19

NameNode và DataNode: Trong mô hình của đồ án, NameNode quản lý metadata (thông tin về vị trí các file CSV), trong khi DataNode thực sự lưu trữ các bit dữ liệu. Việc sử dụng HDFS cho phép đồ án mở rộng dung lượng lưu trữ lên hàng Petabyte chỉ bằng việc bổ sung thêm các máy tính thông thường vào cụm cluster.21

### 2.3. Apache Spark và Mô hình tính toán trong bộ nhớ

Nếu Hadoop là kho chứa, thì Apache Spark là động cơ xử lý. Spark khắc phục nhược điểm lớn nhất của MapReduce là phải ghi dữ liệu xuống đĩa cứng sau mỗi bước tính toán.

Resilient Distributed Datasets (RDD): Spark sử dụng RDD, một cấu trúc dữ liệu trừu tượng cho phép lưu trữ dữ liệu ngay trên RAM của các máy tính trong cụm. Điều này cực kỳ quan trọng đối với các thuật toán học máy (Machine Learning) như Gradient Boosting, vốn đòi hỏi việc lặp đi lặp lại qua tập dữ liệu hàng trăm lần để tối ưu hóa mô hình. Spark có thể nhanh hơn Hadoop MapReduce tới 100 lần trong các tác vụ này.23

Spark SQL & DataFrames: Đồ án sử dụng Spark SQL để thao tác dữ liệu dưới dạng bảng (DataFrame), cho phép thực hiện các truy vấn lọc, nhóm, và biến đổi dữ liệu (như trong file train_model_spark.py) một cách trực quan và tối ưu hóa nhờ Catalyst Optimizer.11

### 2.4. Thuật toán Gradient Boosted Trees (GBT) cho bài toán hồi quy

Để dự đoán tốc độ xe (một biến liên tục), đồ án lựa chọn thuật toán Gradient Boosted Trees (GBTRegressor) thuộc thư viện Spark MLlib.

Lý thuyết: GBT là một kỹ thuật học máy tập hợp (ensemble learning). Nó xây dựng mô hình bằng cách kết hợp nhiều "người học yếu" (weak learners) - thường là các cây quyết định nông (shallow decision trees). Cây thứ nhất học dữ liệu gốc. Cây thứ hai không học lại từ đầu mà học để sửa lỗi (residuals) của cây thứ nhất. Cây thứ ba sửa lỗi của tổng hợp cây 1 và 2. Quá trình này lặp lại cho đến khi đạt số lượng cây quy định (maxIter).25

Hàm mất mát (Loss Function): GBT tối ưu hóa hàm mục tiêu bằng cách sử dụng Gradient Descent. Với bài toán dự báo tốc độ, hàm mất mát thường là Sai số bình phương trung bình (MSE - Mean Squared Error):


Tại mỗi bước, mô hình tính đạo hàm (gradient) của hàm mất mát này để xác định hướng điều chỉnh cho cây tiếp theo.27

Tại sao chọn GBT thay vì XGBoost?

Vì GBT phù hợp xử lý dữ liệu trên Hadoop hơn là XGBoost, mặc dù với tập dữ liệu nhỏ thì XGBoost có phần vượt trội hơn ở độ chính xác so với GBT

| Tiêu chí | XGBoost (Python thuần) | Spark GBTRegressor (PySpark) |

|---|---|---|

| Thư viện | xgboost | pyspark.ml.regression |

| Cơ chế xử lý | In-memory (Load toàn bộ RAM) | Distributed (Phân tán trên Cluster) |

| Nguồn dữ liệu | Đọc file từ ổ cứng (Local Disk) | Đọc file từ HDFS (Hadoop) |

| Input Features | Dùng Pandas DataFrame trực tiếp | Phải gom cột thành Vector (VectorAssembler) |

| Lưu Model | File nén .pkl (nhẹ, vài MB) | Thư mục chứa Metadata & Data (nặng hơn) |

| Triển khai App | Rất dễ, khởi động tức thì | Cần cài Java, Spark, khởi động chậm (5-10s) |









## CHƯƠNG 3: THIẾT KẾ HỆ THỐNG VÀ KIẾN TRÚC CHI TIẾT



Chương này sẽ phân tích chi tiết sơ đồ kiến trúcvà cách thức các thành phần tương tác với nhau để tạo nên hệ thống hoàn chỉnh.

### 3.1. Sơ đồ Kiến trúc Tổng thể (Architecture Breakdown)

Hệ thống được triển khai trên nền tảng hệ điều hành Ubuntu sử dụng ngôn ngữ lập trình Python. Kiến trúc được chia thành 5 tầng (Layers) riêng biệt, đảm bảo tính mô-đun hóa và dễ dàng bảo trì:

| Tầng (Layer) | Thành phần Công nghệ | Chức năng chi tiết |

|---|---|---|

| Data Source Layer | TomTom API | Đây là nguồn dữ liệu đầu vào. Hệ thống sử dụng REST API của TomTom để lấy dữ liệu "Flow Segment" (đoạn dòng chảy), bao gồm tốc độ thực tế, tốc độ tự do, và tọa độ GPS.30 |

| Data Storage Layer | HDFS (Hadoop) | Dữ liệu thô sau khi thu thập (định dạng JSON/CSV) được đẩy vào HDFS. Tại đây, dữ liệu được phân mảnh và lưu trữ an toàn, sẵn sàng cho việc xử lý lô.22 |

| Data Processing Layer | Apache Spark | Spark đóng vai trò trung tâm trong việc ETL (Extract-Transform-Load). Nó đọc dữ liệu từ HDFS, làm sạch (xử lý null, ép kiểu), và tạo các đặc trưng mới (feature engineering).11 |

| Machine Learning Layer | XGBoost / GBT (Spark MLlib) | Tầng này thực hiện việc huấn luyện mô hình. Mã nguồn sử dụng GBTRegressor của Spark MLlib, một biến thể tương đương của Gradient Boosting tối ưu cho môi trường phân tán Spark.32 |

| Visualization Layer | Streamlit (Python) | Tầng giao diện người dùng. Streamlit tạo ra dashboard web tương tác, tích hợp bản đồ Folium để hiển thị trực quan mức độ ùn tắc.11 |



### 3.2. Quy trình dòng dữ liệu (Data Flow Pipeline)

Quy trình xử lý dữ liệu di chuyển qua các bước sau, tương ứng với các file code của đồ án:

Thu thập (Crawl): Script traffic_data_crawl.py chạy như một tiến trình nền (daemon). Nó liên tục gửi request đến TomTom API định kỳ (ví dụ: mỗi 2 phút). Dữ liệu trả về được phân tích cú pháp (parse) và ghi thêm (append) vào file CSV cục bộ, sau đó được đồng bộ lên HDFS.

Làm sạch & Biến đổi (Transform): Script train_model_spark.py khởi động một Spark Session. Nó tải toàn bộ dữ liệu lịch sử từ HDFS vào một Spark DataFrame. Các biến đổi học máy được áp dụng: chuyển đổi cột thời gian (Timestamp) thành các đặc trưng số học (Giờ, Phút, Ngày trong tuần) để mô hình có thể hiểu được tính chu kỳ của giao thông.

Huấn luyện (Training): Dữ liệu được chia thành tập huấn luyện (Train set) và tập kiểm thử (Test set). Mô hình GBT học mối quan hệ giữa các biến đầu vào (Vị trí, Thời gian) và biến mục tiêu (Tốc độ). Mô hình sau khi huấn luyện được lưu (serialize) xuống đĩa để tái sử dụng.

Dự báo & Hiển thị (Serving): Khi người dùng truy cập dashboard (app_spark.py), hệ thống sẽ tải mô hình đã lưu lên RAM. Dựa vào thời gian và địa điểm người dùng chọn, mô hình dự báo tốc độ xe và hiển thị kết quả lên bản đồ số.

## CHƯƠNG 4: THUYẾT MINH CÁC BƯỚC THỰC HIỆN CHI TIẾT



Chương này đi sâu vào mã nguồn và dữ liệu thực tế, giải thích từng bước thực hiện đồ án.

### 4.1. Bước 1: Thu thập Dữ liệu (Data Ingestion) - traffic_data_crawl.py

Đây là bước khởi đầu quan trọng nhất, quyết định chất lượng của toàn bộ hệ thống.

Cấu hình API: Đoạn mã sử dụng API Key từ TomTom. Endpoint được sử dụng là flowSegmentData/relative/10/json. Tham số 10 đại diện cho mức độ zoom, phù hợp để lấy dữ liệu chi tiết cấp đường phố nhưng không quá vụn vặt.11





Cơ chế chịu lỗi (Fault Tolerance): Mạng internet không phải lúc nào cũng ổn định. Script sử dụng cơ chế MAX_RETRIES = 5. Nếu request thất bại (do mất mạng hoặc lỗi server), hệ thống sẽ đợi 10 giây (time.sleep(10)) và thử lại. Điều này đảm bảo quá trình thu thập dữ liệu diễn ra liên tục 24/7 mà không bị ngắt quãng bởi các lỗi mạng thoáng qua.





Cấu trúc dữ liệu thu thập: Dữ liệu được lưu vào file traffic_data_from_traffic.csv bao gồm các trường quan trọng 11:

CurrentSpeed: Tốc độ hiện tại của dòng xe.

FreeFlowSpeed: Tốc độ lý tưởng khi không có xe. Đây là tham số tham chiếu quan trọng để tính toán chỉ số tắc nghẽn.

Confidence: Độ tin cậy của dữ liệu (thường là 1.0 hoặc xấp xỉ 1.0), cho thấy dữ liệu từ TomTom có chất lượng cao.





### 4.2. Bước 2: Quản lý Dữ liệu trên HDFS

Quan sát hình ảnh giao diện HDFS, ta thấy cấu trúc tổ chức dữ liệu của đồ án:



Đường dẫn: /traffic_project/data/.

Phân mảnh dữ liệu: Các file traffic_data...csv được lưu trữ theo ngày (ví dụ: 2025-11-07, 2025-11-08). Cách tổ chức này (partitioning by date) là một "best practice" trong Big Data, giúp Spark dễ dàng lọc và chỉ đọc dữ liệu của những ngày cần thiết thay vì quét toàn bộ kho dữ liệu, tối ưu hóa hiệu năng truy vấn.33

Block Size: Kích thước hiển thị là 128 MB, đây là kích thước block tiêu chuẩn của HDFS, giúp tối ưu hóa băng thông đọc/ghi tuần tự cho các ổ cứng cơ học (HDD) thường dùng trong các cụm server.19

### 4.3. Bước 3: Xử lý và Huấn luyện Mô hình với Spark - train_model_spark.py

Đây là "bộ não" của hệ thống, nơi dữ liệu thô biến thành tri thức.

Kỹ thuật Feature Engineering (Tạo đặc trưng):



Dữ liệu thời gian (Timestamp) không thể đưa trực tiếp vào mô hình toán học. Đoạn mã đã thực hiện một bước xử lý thông minh:
TimeInMinutes = col("Hour") * 60 + col("Minute")

Ý nghĩa: Việc chuyển đổi giờ:phút thành một con số liên tục (từ 0 đến 1439) giúp mô hình GBT hiểu được sự liên tục của thời gian. Ví dụ, 06:59 (419 phút) và 07:00 (420 phút) là rất gần nhau. Nếu để nguyên dạng text hoặc category, mô hình có thể hiểu sai khoảng cách này.

Thêm đặc trưng DayOfWeek và Weekend (Cuối tuần) giúp mô hình phân biệt được sự khác biệt hành vi giao thông giữa ngày làm việc và ngày nghỉ.11



Vector Assembler: Spark MLlib yêu cầu tất cả các đặc trưng đầu vào phải được gộp thành một vector duy nhất. Lệnh VectorAssembler(inputCols=..., outputCol="features") thực hiện việc này, chuẩn bị dữ liệu cho thuật toán GBT.





Cấu hình Mô hình GBTRegressor:

maxIter=100: Mô hình sẽ xây dựng 100 cây quyết định tuần tự. Số lượng cây càng lớn, mô hình càng khớp với dữ liệu huấn luyện (giảm bias) nhưng có nguy cơ học vẹt (overfitting). Con số 100 là mức cân bằng hợp lý.

maxDepth=5: Độ sâu của mỗi cây tối đa là 5 tầng. Cây nông giúp mô hình tổng quát hóa tốt hơn và tránh bị nhiễu bởi các điểm dữ liệu bất thường (outliers) trong dữ liệu giao thông.32





Đánh giá Mô hình (RMSE): Hệ thống sử dụng Root Mean Squared Error (Căn bậc hai sai số bình phương trung bình) để đánh giá.

RMSE phạt nặng các sai số lớn. Trong dự báo giao thông, việc dự báo sai lệch lớn (ví dụ: thực tế kẹt xe mà dự báo đường thoáng) là rất tai hại, nên RMSE là thước đo phù hợp hơn MAE (Mean Absolute Error).35





### 4.4. Bước 4: Xây dựng Ứng dụng Dashboard - app_spark.py

Ứng dụng Streamlit là nơi kết tinh của toàn bộ quá trình xử lý.





Batch Prediction (Dự báo theo lô): Khi người dùng chọn một thời điểm trên sidebar, ứng dụng không gọi API TomTom (vì đó là dữ liệu quá khứ hoặc tương lai). Thay vào đó, nó tạo ra một DataFrame giả lập chứa tất cả các địa điểm cần theo dõi tại thời điểm đó và gửi vào mô hình Spark để dự báo (model.transform(batch_sdf)). Kỹ thuật này tận dụng khả năng xử lý song song của Spark ngay cả trong giai đoạn phục vụ (inference).11

Chỉ số tắc nghẽn (Congestion Ratio):
Hàm get_status_color định nghĩa logic nghiệp vụ cốt lõi:







Nếu Ratio < 0.4 (Tốc độ thực tế < 40% tốc độ lý tưởng) -> Đỏ (Tắc nghẽn nghiêm trọng).

Nếu 0.4 <= Ratio < 0.7 -> Cam (Đông xe).

Nếu Ratio >= 0.7 -> Xanh (Thông thoáng).
Đây là cách tiếp cận chuẩn mực trong kỹ thuật giao thông để định lượng mức độ phục vụ (Level of Service - LOS) của đường bộ.37

Heatmap (Bản đồ nhiệt): Dữ liệu dự báo được trực quan hóa bằng lớp HeatMap của thư viện Folium. Trọng số (weight) cho bản đồ nhiệt được tính bằng 1 - (PredictedSpeed / 60). Điều này có nghĩa là tốc độ càng thấp, trọng số càng cao (gần 1), làm cho khu vực đó hiện màu đỏ rực trên bản đồ, giúp người quản lý nhanh chóng phát hiện điểm nóng ùn tắc.11

## CHƯƠNG 5: PHÂN TÍCH KẾT QUẢ VÀ DỮ LIỆU THỰC NGHIỆM

Dựa trên các tệp dữ liệu CSV đã thu thập (process_data_spark.csv và traffic_data_from_traffic.csv), chúng ta có thể rút ra những nhận định sâu sắc về đặc thù giao thông tại TP.HCM:

### 5.1. Phân tích Xu hướng Thời gian (Temporal Trends)

Dữ liệu cho thấy rõ quy luật "hai đỉnh" của giao thông đô thị:

Đỉnh sáng (06:30 - 08:30): Các dòng dữ liệu (rows 1-5 trong process_data_spark.csv) cho thấy tốc độ bắt đầu giảm mạnh. Ví dụ, tại tọa độ 10.74, 106.61 lúc 06:45, tốc độ thực tế là 25km/h so với mức tự do 35km/h (tỷ lệ 0.71). Đây là giai đoạn người dân đi làm/đi học.

Đỉnh chiều (16:30 - 18:30): Đây là giai đoạn tồi tệ nhất. Dữ liệu tại dòng 30 (17:13) ghi nhận tốc độ sụt giảm xuống còn 12 km/h so với mức tự do 29 km/h (tỷ lệ 0.41 - gần chạm mức tắc nghẽn nghiêm trọng).

Giờ thấp điểm: Giữa trưa (11:00 - 13:00), tốc độ thường phục hồi về mức tiệm cận tốc độ tự do (Ratio ~ 1.0), phản ánh sự giảm tải của hạ tầng.11

### 5.2. Phân tích Không gian (Spatial Trends)

Dữ liệu từ file 2025-11-07...csv 11 cho thấy sự chênh lệch rõ rệt giữa các loại đường:

Cầu và Đại lộ: Các vị trí như "Cầu Thủ Thiêm" hay đại lộ "Mai Chí Thọ" thường duy trì tốc độ cao (40-50 km/h) do ít giao cắt.

Trung tâm (Quận 1, Quận 3): Các tuyến đường như "Nguyễn Huệ", "Nam Kỳ Khởi Nghĩa" có tốc độ trung bình thấp và biến động mạnh, do mật độ giao cắt dày đặc và hoạt động thương mại sầm uất.

### 5.3. Đánh giá Mô hình GBT

Việc sử dụng mô hình phi tuyến tính GBT là hoàn toàn phù hợp. Dữ liệu cho thấy mối quan hệ giữa "Thời gian" và "Tốc độ" không phải là đường thẳng mà là các đường cong phức tạp với các điểm gãy đột ngột (ví dụ: đường đang thoáng bỗng nhiên tắc cứng chỉ sau 15 phút). Cây quyết định (Decision Trees) trong GBT có khả năng học các điểm gãy này ("IF hour > 17 AND hour < 18 THEN speed = low") tốt hơn nhiều so với Hồi quy tuyến tính hay ARIMA đơn thuần.34



## CHƯƠNG 6: ĐÁNH GIÁ, ƯU KHUYẾT ĐIỂM VÀ HƯỚNG PHÁT TRIỂN

### 6.1. Ưu điểm (Pros)

Tính Quy mô và Mở rộng (Scalability): Điểm mạnh lớn nhất của hệ thống là việc sử dụng kiến trúc Hadoop/Spark. Khi dữ liệu giao thông của TP.HCM tăng từ vài Gigabyte lên Terabyte (do mở rộng giám sát ra ngoại thành hoặc tăng tần suất lấy mẫu), ta chỉ cần thêm node vào cụm HDFS/Spark mà không cần viết lại mã nguồn. Đây là đặc tính "Scale-out" mà các hệ thống cơ sở dữ liệu truyền thống không có.19

Hiệu năng xử lý cao: Nhờ cơ chế tính toán In-Memory của Spark, thời gian huấn luyện mô hình được rút ngắn đáng kể, cho phép cập nhật mô hình thường xuyên (ví dụ: hàng tuần) để thích ứng với những thay đổi về hạ tầng giao thông (lô cốt, đường cấm mới).

Trực quan hóa hiệu quả: Giao diện Streamlit cung cấp cái nhìn tức thời và dễ hiểu thông qua Heatmap và mã màu (Xanh/Cam/Đỏ). Người điều hành không cần kiến thức chuyên sâu về dữ liệu vẫn có thể ra quyết định điều phối giao thông.

Chi phí thấp: Giải pháp tận dụng dữ liệu Floating Car Data (FCD) từ API thay vì đầu tư hàng triệu USD để lắp đặt cảm biến từ tính (inductive loops) hay camera tại mọi ngã tư.

### 6.2. Khuyết điểm và Hạn chế (Cons)

Độ trễ cập nhật (Latency): Mô hình hiện tại hoạt động theo cơ chế Batch (xử lý lô). Dữ liệu thu thập phải đợi đủ một khoảng thời gian (ví dụ: 1 ngày) mới được đưa vào huấn luyện lại. Nếu có một sự kiện bất thường xảy ra ngay bây giờ (ví dụ: tai nạn liên hoàn), mô hình GBT đã huấn luyện từ hôm qua sẽ không biết và vẫn dự báo đường thoáng. Thiếu lớp xử lý sự kiện thời gian thực (Speed Layer) thực sự.14

Phụ thuộc bên thứ 3: Hệ thống phụ thuộc hoàn toàn vào TomTom API. Nếu API này thay đổi chính sách giá, giới hạn request (Rate Limit), hoặc bị gián đoạn, hệ thống sẽ "mù" thông tin. Giới hạn 2500 request/ngày của gói miễn phí là rào cản cho việc triển khai diện rộng.40

Thiếu biến ngoại sinh (Exogenous Variables): Mô hình hiện tại chỉ dự báo dựa trên Thời gian và Vị trí. Thực tế, giao thông TP.HCM chịu ảnh hưởng cực lớn từ Mưa và Triều cường. Việc thiếu dữ liệu thời tiết (Weather API) làm giảm độ chính xác của dự báo trong những ngày mưa bão.42

### 6.3. Hướng phát triển trong tương lai (Future Work)

Nâng cấp lên Kiến trúc Streaming (Real-time):

Tích hợp Apache Kafka để làm bộ đệm tiếp nhận dữ liệu thời gian thực (Message Broker).

Sử dụng Spark Structured Streaming để xử lý dữ liệu ngay khi nó vừa đến (on-the-fly), cho phép phát hiện sự cố tức thời thay vì chờ xử lý lô.17

Mô hình học sâu tiên tiến (Deep Learning):

Triển khai mạng nơ-ron hồi quy LSTM (Long Short-Term Memory) để nắm bắt tốt hơn các chuỗi phụ thuộc thời gian dài (long-term dependencies).

Nghiên cứu áp dụng Graph Neural Networks (GNN). Mạng giao thông thực chất là một đồ thị (Graph), nơi các ngã tư là nút (nodes) và con đường là cạnh (edges). GNN có thể mô hình hóa việc tắc nghẽn ở nút A lan truyền sang nút B như thế nào, điều mà các mô hình bảng (tabular) như GBT khó làm được.44

Tích hợp Bản sao số (Digital Twin):

Xây dựng mô hình 3D mô phỏng giao thông thời gian thực (như các dự án tại cảng biển Việt Nam đang áp dụng). Điều này cho phép thực hiện các kịch bản giả định (Simulation): "Điều gì xảy ra nếu cấm xe hơi vào đường Nguyễn Huệ?" trước khi áp dụng thực tế.46

Đa dạng hóa nguồn dữ liệu:

Kết hợp dữ liệu mưa/triều cường từ các trạm khí tượng thủy văn.

Tích hợp dữ liệu sự kiện xã hội (lịch thi đấu bóng đá, lễ hội) để dự báo các đợt tăng đột biến lưu lượng xe.48



## NGUỒN TRÍCH DẪN



Thiệt hại hàng tỉ USD vì hạ tầng quá tải, những giải pháp TP.HCM cần làm ngay, truy cập vào tháng 12 15, 2025, https://tuoitre.vn/thiet-hai-hang-ti-usd-vi-ha-tang-qua-tai-nhung-giai-phap-tp-hcm-can-lam-ngay-20250807095909853.htm

Đại biểu Nguyễn Văn Cảnh: TP.HCM thiệt hại khoảng 6 tỉ USD/năm do ùn tắc giao thông, truy cập vào tháng 12 15, 2025, https://plo.vn/dai-bieu-nguyen-van-canh-tphcm-thiet-hai-khoang-6-ti-usdnam-do-un-tac-giao-thong-post852233.html

Ùn tắc giao thông ở TP.HCM tăng 24%, vận tốc trung bình gần 35km/h - PLO, truy cập vào tháng 12 15, 2025, https://plo.vn/un-tac-giao-thong-o-tphcm-tang-24-van-toc-trung-binh-gan-35kmh-post884920.html

TPHCM: Ùn tắc giao thông tăng 24% so với cùng kỳ năm 2024 | BÁO SÀI GÒN GIẢI PHÓNG, truy cập vào tháng 12 15, 2025, https://www.sggp.org.vn/tphcm-un-tac-giao-thong-tang-24-so-voi-cung-ky-nam-2024-post826941.html

TPHCM bùng phát ùn tắc giao thông - VietNamNet, truy cập vào tháng 12 15, 2025, https://vietnamnet.vn/tphcm-bung-phat-un-tac-giao-thong-so-xay-dung-diem-mat-loat-nguyen-nhan-2469493.html

TP.HCM có hơn 7,6 triệu xe máy, tạo ra áp lực giao thông thế nào? - PLO, truy cập vào tháng 12 15, 2025, https://plo.vn/tphcm-co-hon-76-trieu-xe-may-tao-ra-ap-luc-giao-thong-the-nao-post806429.html

BÁO CÁO VỀ TÌNH HÌNH GIAO THÔNG TẠI TP.HCM | PDF - Scribd, truy cập vào tháng 12 15, 2025, https://www.scribd.com/document/839910916/BAO-CAO-V%E1%BB%80-TINH-HINH-GIAO-THONG-T%E1%BA%A0I-TP-HCM

Big Data – Dữ liệu lớn ảnh hưởng đến thành phố thông minh như thế nào? - East Mount, truy cập vào tháng 12 15, 2025, https://eastmount.vn/2020/08/08/big-data-du-lieu-lon-anh-huong-den-thanh-pho-thong-minh-nhu-the-nao/

Batch Processing vs Stream Processing: Key Differences & Use Cases - Estuary, truy cập vào tháng 12 15, 2025, https://estuary.dev/blog/batch-processing-vs-stream-processing/

Batch vs Stream Processing: When to Use Each and Why It Matters - DataCamp, truy cập vào tháng 12 15, 2025, https://www.datacamp.com/blog/batch-vs-stream-processing

traffic_data_crawl.py

Plan on sustainable development of smart cities nodded - Vietnam Law and Legal Forum, truy cập vào tháng 12 15, 2025, https://vietnamlawmagazine.vn/plan-on-sustainable-development-of-smart-cities-nodded-6359.html

Ho Chi Minh City unveils blueprint for future megacity - VietNamNet, truy cập vào tháng 12 15, 2025, https://vietnamnet.vn/en/ho-chi-minh-city-unveils-blueprint-for-future-megacity-2450231.html

The Basics of Lambda Architecture for Big Data - Snowflake, truy cập vào tháng 12 15, 2025, https://www.snowflake.com/en/fundamentals/lambda-architecture/

What Is Lambda Architecture? - Coursera, truy cập vào tháng 12 15, 2025, https://www.coursera.org/articles/lambda-architecture

Understanding Lambda Architecture: A Scalable Solution for Big Data Processing - Medium, truy cập vào tháng 12 15, 2025, https://medium.com/@devendra631995/understanding-lambda-architecture-a-scalable-solution-for-big-data-processing-ad06577fe839

Spark vs Kafka - Difference Between Data Processing Engines - AWS, truy cập vào tháng 12 15, 2025, https://aws.amazon.com/compare/the-difference-between-kafka-and-spark/

Understanding Streamlit's client-server architecture, truy cập vào tháng 12 15, 2025, https://docs.streamlit.io/develop/concepts/architecture/architecture

Explain the Hadoop Distributed File System (HDFS) Architecture and Advantages., truy cập vào tháng 12 15, 2025, https://www.geeksforgeeks.org/data-engineering/explain-the-hadoop-distributed-file-system-hdfs-architecture-and-advantages/

The Architecture of Open Source Applications (Volume 1)The Hadoop Distributed File System, truy cập vào tháng 12 15, 2025, https://aosabook.org/en/v1/hdfs.html

Hadoop HDFS Architecture Explanation and Assumptions - DataFlair, truy cập vào tháng 12 15, 2025, https://data-flair.training/blogs/hadoop-hdfs-architecture/

What is Hadoop Distributed File System (HDFS)? - IBM, truy cập vào tháng 12 15, 2025, https://www.ibm.com/think/topics/hdfs

Apache Spark Architecture: A Guide for Data Practitioners | DataCamp, truy cập vào tháng 12 15, 2025, https://www.datacamp.com/blog/apache-spark-architecture

Apache Spark architecture: Concepts, components, and best practices - NetApp Instaclustr, truy cập vào tháng 12 15, 2025, https://www.instaclustr.com/education/apache-spark/apache-spark-architecture-concepts-components-and-best-practices/

What is the XGBoost algorithm and how does it work? - Analytics Vidhya, truy cập vào tháng 12 15, 2025, https://www.analyticsvidhya.com/blog/2018/09/an-end-to-end-guide-to-understand-the-math-behind-xgboost/

A Gentle Introduction to Gradient Boosting - Khoury College of Computer Sciences, truy cập vào tháng 12 15, 2025, https://www.khoury.northeastern.edu/home/vip/teach/MLcourse/4_boosting/slides/gradient_boosting.pdf

Introduction to Boosted Trees — xgboost 3.1.1 documentation, truy cập vào tháng 12 15, 2025, https://xgboost.readthedocs.io/en/stable/tutorials/model.html

Supervised Machine Learning with Gradient Boosting - University of San Diego Online Degrees, truy cập vào tháng 12 15, 2025, https://onlinedegrees.sandiego.edu/supervised-machine-learning-with-gradient-boosting/

Traffic Prediction Using LSTM, RF and XGBoost - SciTePress, truy cập vào tháng 12 15, 2025, https://www.scitepress.org/Papers/2024/135156/135156.pdf

Traffic APIs - TomTom, truy cập vào tháng 12 15, 2025, https://www.tomtom.com/products/traffic-apis/

Flow Segment Data | Traffic API - TomTom Developer Portal, truy cập vào tháng 12 15, 2025, https://developer.tomtom.com/traffic-api/documentation/tomtom-maps/traffic-flow/flow-segment-data

GBTRegressionModel — PySpark 4.0.1 documentation - Apache Spark, truy cập vào tháng 12 15, 2025, https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.regression.GBTRegressionModel.html

What is Hadoop Distributed File System (HDFS) - Databricks, truy cập vào tháng 12 15, 2025, https://www.databricks.com/glossary/hadoop-distributed-file-system-hdfs

Mastering Gradient-Boosted Tree Regressors in PySpark: A Comprehensive Guide, truy cập vào tháng 12 15, 2025, https://www.sparkcodehub.com/pyspark/mllib/gbt-regressor

Root Mean Square Error (RMSE) - Statistics By Jim, truy cập vào tháng 12 15, 2025, https://statisticsbyjim.com/regression/root-mean-square-error-rmse/

Root Mean Square Error (RMSE) in Machine Learning - Applied AI Course, truy cập vào tháng 12 15, 2025, https://www.appliedaicourse.com/blog/root-mean-square-error-rmse/

Measuring Traffic Congestion - eScholarship, truy cập vào tháng 12 15, 2025, https://escholarship.org/content/qt2hr1k5s0/qt2hr1k5s0_noSplash_a024e9f44dc1af4b615f52f6e658e735.pdf

Congestion Measures | KYTC - Kentucky.gov, truy cập vào tháng 12 15, 2025, https://transportation.ky.gov/Congestion-Toolbox/Pages/Congestion-Measures.aspx

Short-Term Traffic Flow Prediction Based on XGBoost - Semantic Scholar, truy cập vào tháng 12 15, 2025, https://www.semanticscholar.org/paper/Short-Term-Traffic-Flow-Prediction-Based-on-XGBoost-Dong-Lei/99b50323d22bcf4fe6c4dc8ce1199851fa734fa9

Pricing - TomTom Developer Portal, truy cập vào tháng 12 15, 2025, https://developer.tomtom.com/pricing

PSA: TomTom has 2500 free daily requests for their maps/traffic api...GREAT for a work commute traffic check script! - Reddit, truy cập vào tháng 12 15, 2025, https://www.reddit.com/r/selfhosted/comments/1bzkmzg/psa_tomtom_has_2500_free_daily_requests_for_their/

Improving Urban Traffic Speed Prediction Using Data Source Fusion and Deep Learning - SciSpace, truy cập vào tháng 12 15, 2025, https://scispace.com/pdf/improving-urban-traffic-speed-prediction-using-data-source-422pt4arwk.pdf

How To Use Apache Spark and Kafka® for Machine Learning Part 1 - NetApp Instaclustr, truy cập vào tháng 12 15, 2025, https://www.instaclustr.com/blog/apache-spark-and-apache-kafka/

A Graph Neural Network- based Traffic Flow Prediction System with Enhanced Accuracy and Urban Efficiency, truy cập vào tháng 12 15, 2025, https://journal.esrgroups.org/jes/article/download/642/664/1095

Advancements of Graph Neural Networks in Urban Traffic Prediction - SciTePress, truy cập vào tháng 12 15, 2025, https://www.scitepress.org/Papers/2024/129022/129022.pdf

How Digital Twin Shape Smart Traffic - Trustify Technology, truy cập vào tháng 12 15, 2025, https://www.trustifytechnology.com/news/how-digital-twins-shape-smart-traffic/

Vietnam's Strategic Use of Digital Twins for Port Development | Fast Company - Esri, truy cập vào tháng 12 15, 2025, https://www.esri.com/en-us/lg/brand/stories/vietnams-strategic-use-of-digital-twins-for-port-development

Research Support - Vietnamese-German University, truy cập vào tháng 12 15, 2025, https://vgu.edu.vn/research-support?title=Seminar-DIGITAL-TWINS-FOR-SMART-URBAN-TRAFFIC-MANAGEMENT&id-bai-viet=11479028&pid=CmsHienThiBaiViet_WAR_cmsportlet_INSTANCE_kxumshm4Wie3&reCall=1