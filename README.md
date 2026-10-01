# LAB 05 - Traffic Flow Analysis and Prediction

Dự án xây dựng hệ thống phân tích và dự đoán lưu lượng giao thông đô thị tại TP.HCM. Hệ thống sử dụng dữ liệu giao thông thu thập theo tọa độ, tiền xử lý dữ liệu, huấn luyện mô hình dự đoán tốc độ xe, và hiển thị kết quả trên dashboard Streamlit với bản đồ Folium.

## Công nghệ sử dụng

- Python
- Streamlit
- Pandas, NumPy
- Folium, streamlit-folium
- XGBoost, scikit-learn
- PySpark / Spark MLlib
- Hadoop HDFS
- TomTom Traffic API

## Cấu trúc thư mục

```text
LAB5/
├── Code & Model/
│   ├── app.py                    # Dashboard Streamlit dùng model XGBoost
│   ├── app_spark.py              # Dashboard Streamlit dùng Spark MLlib
│   ├── traffic_data_crawl.py     # Thu thập dữ liệu từ TomTom API
│   ├── preprocess_folder.py      # Tiền xử lý nhiều file CSV bằng Pandas
│   ├── process_spark.py          # Tiền xử lý dữ liệu trên HDFS bằng Spark
│   ├── train_model_xgboost.py    # Huấn luyện model XGBoost
│   ├── train_model_GBT.py        # Huấn luyện model GBT bằng Spark MLlib
│   ├── xgboost_model.pkl         # Model XGBoost đã huấn luyện
│   └── train_model_spark/        # Model Spark đã huấn luyện
├── Data & process_data/
│   ├── data_converted.csv        # Danh sách địa điểm/tọa độ
│   ├── process_data_spark.csv    # Dữ liệu đã xử lý cho Spark
│   ├── data/                     # Dữ liệu thô theo ngày
│   └── preprocess data/          # Dữ liệu đã tiền xử lý
├── LAB05.docx
├── LAB05.md
├── requirements.txt
└── README.md
```

## Cài đặt

Nên tạo môi trường ảo trước khi cài thư viện.

```powershell
cd "C:\Users\Admin\Desktop\AI TOXIC\LAB5"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
# Mở file .env và điền TOMTOM_API_KEY nếu cần thu thập dữ liệu mới
```

Nếu PowerShell chặn chạy script activate, chạy lệnh sau rồi thử lại:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

## Chạy dashboard XGBoost

Đây là cách chạy nhanh và ổn định nhất để demo dự án.

`app.py` cần file `xgboost_model.pkl` và `data_converted.csv` nằm cùng thư mục với file app. Model đã có sẵn trong `Code & Model`, nhưng file `data_converted.csv` đang nằm trong `Data & process_data`, vì vậy cần copy trước khi chạy.

```powershell
cd "C:\Users\Admin\Desktop\AI TOXIC\LAB5"
Copy-Item "Data & process_data\data_converted.csv" "Code & Model\data_converted.csv" -Force
cd "Code & Model"
streamlit run app.py
```

Sau khi chạy, Streamlit sẽ hiển thị URL trên terminal, thường là:

```text
http://localhost:8501
```

## Train lại model XGBoost

Script `train_model_xgboost.py` đọc dữ liệu từ thư mục `./preprocess data/*.csv`. Thư mục này hiện nằm trong `Data & process_data`, nên nên chạy script từ thư mục đó:

```powershell
cd "C:\Users\Admin\Desktop\AI TOXIC\LAB5\Data & process_data"
python "..\Code & Model\train_model_xgboost.py"
```

Sau khi train xong, model mới sẽ được tạo tại:

```text
Data & process_data/xgboost_model.pkl
```

Copy model mới sang thư mục app:

```powershell
Copy-Item "xgboost_model.pkl" "..\Code & Model\xgboost_model.pkl" -Force
```

## Tiền xử lý dữ liệu bằng Pandas

Nếu có các file CSV thô trong `Data & process_data/data`, có thể tiền xử lý bằng:

```powershell
cd "C:\Users\Admin\Desktop\AI TOXIC\LAB5\Data & process_data"
python "..\Code & Model\preprocess_folder.py"
```

Kết quả sẽ được lưu vào:

```text
Data & process_data/preprocess data/
```

## Chạy phần Spark/HDFS

Phần Spark phù hợp khi cần minh họa pipeline Big Data đầy đủ. Cần cài thêm:

- Java JDK
- Apache Spark / PySpark
- Hadoop HDFS đang chạy tại `hdfs://localhost:9000`

Tiền xử lý dữ liệu trên HDFS:

```powershell
cd "C:\Users\Admin\Desktop\AI TOXIC\LAB5\Code & Model"
python process_spark.py
```

Train model Spark GBT:

```powershell
python train_model_GBT.py
```

Chạy dashboard Spark:

```powershell
streamlit run app_spark.py
```

Lưu ý: `app_spark.py` đang load model từ path:

```python
model_path = "/traffic_project/train_model_spark"
```

Nếu chạy local trên Windows với model trong repo, cần đổi thành:

```python
model_path = "train_model_spark"
```

## Thu thập dữ liệu từ TomTom API

File `traffic_data_crawl.py` gọi TomTom Traffic API để lấy tốc độ giao thông theo danh sách tọa độ trong `data_converted.csv`.

```powershell
cd "C:\Users\Admin\Desktop\AI TOXIC\LAB5\Code & Model"
python traffic_data_crawl.py
```

Script này chạy liên tục và lấy dữ liệu mới mỗi 2 phút. Dùng `Ctrl + C` để kết thúc.

## Lưu ý quan trọng

- API key TomTom được quản lý qua biến môi trường trong file `.env` (dựa trên mẫu `.env.example`). File `.env` đã được cấu hình trong `.gitignore` để tránh rò rỉ khi đẩy lên GitHub.
- Bản XGBoost dễ chạy demo hơn vì không cần Hadoop/HDFS.
- Bản Spark thể hiện đúng kiến trúc Big Data hơn, nhưng cần cấu hình môi trường nhiều hơn.
- Một số đường dẫn trong code đang phụ thuộc vào working directory. Khi chạy lệnh nên dùng đúng thư mục như hướng dẫn ở trên.

## Kết quả mong đợi

Dashboard hiển thị bản đồ TP.HCM với các điểm giao thông, heatmap mức độ tắc nghẽn, tốc độ dự đoán và trạng thái giao thông tại từng địa điểm.
