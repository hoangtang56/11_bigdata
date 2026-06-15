# Phát hiện Gian lận Giao dịch Thương mại Điện tử trên Nền tảng Big Data với Apache Hadoop và Spark

## 1. Giới thiệu dự án

Đây là đồ án môn **Big Data với mục tiêu xây dựng một pipeline xử lý dữ liệu lớn phục vụ bài toán ****phát hiện gian lận trong giao dịch thương mại điện tử**. Dự án sử dụng **Apache Hadoop HDFS** để lưu trữ dữ liệu phân tán, **Apache Spark** để xử lý và phân tích dữ liệu, **Spark SQL** để khai thác thông tin, và **Spark MLlib** để xây dựng mô hình học máy phân tán.

Bài toán được đặt trong bối cảnh các nền tảng thương mại điện tử cần phát hiện sớm những giao dịch có khả năng gian lận nhằm giảm thiểu tổn thất tài chính, hỗ trợ kiểm soát rủi ro và nâng cao hiệu quả vận hành.

## 2. Thành viên nhóm

* Tăng Gia Hoàng
* Nguyễn Lợi Thanh Dũng
* Nguyễn Đỗ Nhật Minh
* Trần Huỳnh Huy Thông

## 3. Mục tiêu thực hiện

Dự án tập trung vào các mục tiêu chính:

* Lưu trữ bộ dữ liệu giao dịch thương mại điện tử trên **Hadoop Distributed File System - HDFS**.
* Kết nối **Apache Spark** với HDFS để đọc và xử lý dữ liệu trực tiếp từ hệ thống lưu trữ phân tán.
* Thực hiện phân tích dữ liệu khám phá - **Exploratory Data Analysis (EDA)**.
* Xây dựng các truy vấn phân tích nâng cao bằng **Spark SQL**.
* Xây dựng pipeline học máy phân tán bằng **Spark MLlib**.
* Đánh giá mô hình phát hiện gian lận bằng các chỉ số phù hợp với bài toán dữ liệu mất cân bằng.
* Đề xuất mở rộng ứng dụng Big Data thông qua streaming và tối ưu hóa hiệu năng xử lý phân tán.

## 4. Bộ dữ liệu

Dự án sử dụng bộ dữ liệu giao dịch thương mại điện tử có nhãn gian lận.

| Thuộc tính       | Mô tả                                                     |
| ---------------- | --------------------------------------------------------- |
| Tên dữ liệu      | Fraudulent E-Commerce Transactions                        |
| Nguồn dữ liệu    | Kaggle                                                    |
| Tên file dữ liệu | `fraud_300k_raw_temporal.csv`                             |
| Bài toán         | Phân loại giao dịch gian lận                              |
| Cột nhãn         | `Is Fraudulent`                                           |
| Đường dẫn HDFS   | `hdfs://localhost:9000/final/fraud_300k_raw_temporal.csv` |

Một số nhóm thuộc tính chính trong dữ liệu:

* Thông tin giao dịch: mã giao dịch, số tiền giao dịch, ngày giao dịch, giờ giao dịch.
* Thông tin khách hàng: mã khách hàng, độ tuổi, vị trí, tuổi tài khoản.
* Thông tin sản phẩm: danh mục sản phẩm, số lượng.
* Thông tin thanh toán và thiết bị: phương thức thanh toán, thiết bị sử dụng.
* Thông tin địa chỉ và mạng: IP Address, Shipping Address, Billing Address.
* Nhãn phân loại: giao dịch gian lận hoặc không gian lận.

## 5. Công nghệ sử dụng

| Thành phần            | Công nghệ                                        |
| --------------------- | ------------------------------------------------ |
| Ngôn ngữ lập trình    | Python                                           |
| Lưu trữ phân tán      | Apache Hadoop HDFS                               |
| Xử lý dữ liệu lớn     | Apache Spark                                     |
| Truy vấn dữ liệu      | Spark SQL                                        |
| Học máy phân tán      | Spark MLlib                                      |
| Môi trường phát triển | Jupyter Notebook                                 |
| Thư viện hỗ trợ       | PySpark, Pandas, NumPy, Matplotlib, Scikit-learn |

## 6. Cấu trúc thư mục

```text
11_bigdata/
├── data/
│   └── README hoặc dữ liệu mẫu nếu có
├── docs/
│   └── tài liệu báo cáo, hình ảnh minh chứng, slide nếu có
├── src/
│   ├── chapter2_eda.ipynb
│   ├── chapter4_spark_sql.ipynb
│   ├── chapter5_mllib.ipynb
│   └── chapter6_streaming_prepare.ipynb
├── .gitignore
└── README.md
```

## 7. Mô tả các notebook chính

| File                               | Nội dung                                                                                                                                        |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `chapter2_eda.ipynb`               | Khám phá dữ liệu, kiểm tra kích thước dữ liệu, kiểu dữ liệu, missing values, phân phối nhãn và các đặc trưng quan trọng.                        |
| `chapter4_spark_sql.ipynb`         | Thực hiện các truy vấn phân tích nâng cao bằng Spark SQL, bao gồm aggregation, group by, window function, subquery và phân tích theo thời gian. |
| `chapter5_mllib.ipynb`             | Xây dựng pipeline học máy phân tán bằng Spark MLlib, tiền xử lý dữ liệu, huấn luyện mô hình và đánh giá kết quả.                                |
| `chapter6_streaming_prepare.ipynb` | Chuẩn bị phần mở rộng streaming và/hoặc mô phỏng hướng ứng dụng xử lý dữ liệu giao dịch gần thời gian thực.                                     |

## 8. Quy trình xử lý dữ liệu

Pipeline tổng quát của dự án:

```text
Raw CSV Dataset
        |
        v
Upload to Hadoop HDFS
        |
        v
Read data from HDFS using Apache Spark
        |
        v
EDA and Data Cleaning
        |
        v
Spark SQL Analytics
        |
        v
Spark MLlib Preprocessing Pipeline
        |
        v
Model Training and Evaluation
        |
        v
Business Interpretation and Big Data Extension
```

Các bước chính bao gồm:

1. Đọc dữ liệu giao dịch từ HDFS bằng Spark.
2. Kiểm tra schema, số dòng, số cột và chất lượng dữ liệu.
3. Xử lý missing values và các vấn đề định dạng dữ liệu.
4. Phân tích phân phối dữ liệu, đặc biệt là phân phối nhãn gian lận.
5. Thực hiện các truy vấn Spark SQL để khai thác insight từ dữ liệu.
6. Xây dựng pipeline tiền xử lý với Spark MLlib.
7. Huấn luyện mô hình phân loại giao dịch gian lận.
8. Đánh giá mô hình bằng các chỉ số phù hợp với dữ liệu mất cân bằng.
9. Đề xuất hướng mở rộng Big Data trong thực tế.

## 9. Spark SQL Analytics

Phần Spark SQL được sử dụng để phân tích sâu hành vi giao dịch và các yếu tố liên quan đến gian lận.

Các hướng phân tích chính:

* Tỷ lệ gian lận theo phương thức thanh toán.
* Tỷ lệ gian lận theo danh mục sản phẩm.
* Phân tích số tiền giao dịch trung bình giữa giao dịch gian lận và không gian lận.
* Phân tích giao dịch theo khung giờ.
* Phân tích gian lận theo độ tuổi khách hàng.
* Phân tích theo tuổi tài khoản.
* Phân tích xu hướng giao dịch theo thời gian.
* Sử dụng window function để xếp hạng nhóm có rủi ro cao.
* Sử dụng aggregation để tổng hợp số lượng và giá trị giao dịch.
* Sử dụng subquery để lọc các nhóm giao dịch bất thường.

## 10. Spark MLlib Pipeline

Phần học máy được xây dựng bằng Spark MLlib nhằm đảm bảo quá trình tiền xử lý và huấn luyện có thể thực hiện trên môi trường dữ liệu lớn.

Các bước chính trong pipeline:

* Xử lý missing values.
* Mã hóa biến phân loại bằng `StringIndexer`.
* One-hot encoding với `OneHotEncoder`.
* Kết hợp đặc trưng bằng `VectorAssembler`.
* Chuẩn hóa hoặc biến đổi đặc trưng nếu cần.
* Chia dữ liệu thành tập train/test.
* Huấn luyện mô hình phân loại.
* Đánh giá mô hình bằng Spark Evaluator.

Bài toán được xác định là **binary classification**, trong đó:

* `0`: giao dịch không gian lận.
* `1`: giao dịch gian lận.

## 11. Kết quả mô hình

Do bài toán phát hiện gian lận thường có dữ liệu mất cân bằng, dự án không chỉ tập trung vào Accuracy mà ưu tiên các chỉ số phản ánh tốt khả năng phát hiện lớp gian lận như **Recall**, **Precision**, **F2-score**, **PR-AUC** và **ROC-AUC**.

| Metric    | Kết quả |
| --------- | ------- |
| F2-score  | 0.5353  |
| PR-AUC    | 0.5788  |
| ROC-AUC   | 0.8457  |
| Recall    | 0.6185  |
| Precision | 0.3479  |

Trong bài toán phát hiện gian lận, **F2-score** được xem là chỉ số quan trọng vì chỉ số này đặt trọng số cao hơn cho Recall. Điều này phù hợp với mục tiêu thực tế là giảm số lượng giao dịch gian lận bị bỏ sót.

## 12. Định hướng mở rộng

Dự án đề xuất một số hướng mở rộng phù hợp với bối cảnh Big Data trong thương mại điện tử:

### 12.1. Streaming Fraud Detection

Trong thực tế, các giao dịch thương mại điện tử phát sinh liên tục. Vì vậy, hệ thống có thể được mở rộng theo hướng xử lý streaming để phát hiện giao dịch đáng ngờ gần thời gian thực.

Hướng triển khai:

* Mô phỏng luồng giao dịch mới.
* Đọc dữ liệu theo micro-batch.
* Áp dụng mô hình đã huấn luyện để dự đoán rủi ro gian lận.
* Gắn cờ cảnh báo cho các giao dịch có xác suất gian lận cao.

### 12.2. Tối ưu hóa hiệu năng phân tán

Dự án có thể mở rộng bằng cách áp dụng các kỹ thuật tối ưu hiệu năng trong Spark:

* Sử dụng `cache()` hoặc `persist()` cho các DataFrame được tái sử dụng nhiều lần.
* Phân tích kế hoạch thực thi bằng `explain(True)`.
* Hạn chế chuyển đổi dữ liệu không cần thiết giữa Spark DataFrame và Pandas DataFrame.
* Đọc dữ liệu trực tiếp từ HDFS để giữ đúng bản chất xử lý phân tán.
* Tối ưu số partition khi xử lý dữ liệu lớn.

## 13. Cách chạy dự án

### 13.1. Khởi động Hadoop HDFS

```powershell
start-dfs.cmd
```

Kiểm tra các tiến trình Hadoop:

```powershell
jps
```

Kết quả kỳ vọng có các tiến trình như:

```text
NameNode
DataNode
SecondaryNameNode
```

### 13.2. Tạo thư mục và upload dữ liệu lên HDFS

```powershell
hdfs dfs -mkdir -p /final
hdfs dfs -put fraud_300k_raw_temporal.csv /final/
hdfs dfs -ls /final
```

### 13.3. Chạy notebook

Mở Jupyter Notebook hoặc JupyterLab, sau đó chạy lần lượt các file trong thư mục `src`:

```text
1. chapter2_eda.ipynb
2. chapter4_spark_sql.ipynb
3. chapter5_mllib.ipynb
4. chapter6_streaming_prepare.ipynb
```


## 14. Mục đích sử dụng

Repository này được xây dựng phục vụ mục đích học tập và nghiên cứu trong khuôn khổ môn học **Big Data**.

Dự án không nhằm triển khai trực tiếp trong môi trường production. Để ứng dụng thực tế, cần bổ sung thêm các thành phần như kiểm thử hệ thống, giám sát mô hình, bảo mật dữ liệu, kiểm soát quyền truy cập và đánh giá chi phí vận hành.

## 15. Kết luận

Dự án đã xây dựng một quy trình xử lý dữ liệu lớn cho bài toán phát hiện gian lận giao dịch thương mại điện tử, bao gồm lưu trữ dữ liệu trên HDFS, xử lý bằng Spark, phân tích bằng Spark SQL và huấn luyện mô hình bằng Spark MLlib.

Kết quả cho thấy mô hình có khả năng phân biệt giao dịch gian lận và không gian lận ở mức tương đối tốt thông qua ROC-AUC, đồng thời sử dụng F2-score để phản ánh tốt hơn mục tiêu ưu tiên phát hiện gian lận trong bối cảnh dữ liệu mất cân bằng.
