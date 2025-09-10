package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/joho/godotenv"
	pb "github.com/polyakovWeb/crosspromo_backend_go/cmd/server/assets"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

func getAggStages() []bson.M {
	return []bson.M{
		{
			"$project": bson.M{
				"localizationCode": "$Cluster.LocalizationCode",
				"platforms": bson.M{
					"$map": bson.M{
						"input": bson.A{"$Cluster.Platform"},
						"as":    "platform",
						"in": bson.M{
							"name": "$$platform",
							"AdType": bson.M{
								"Rewarded": bson.M{
									"$let": bson.M{
										"vars": bson.M{
											"crosspromo": bson.M{
												"$arrayElemAt": bson.A{
													bson.M{
														"$filter": bson.M{
															"input": "$AppDynamicData.AdWaterfall.Rewarded",
															"as":    "item",
															"cond":  bson.M{"$eq": bson.A{"$$item.Network", "Crosspromo"}},
														},
													},
													0,
												},
											},
										},
										"in": bson.M{
											"title": "$$crosspromo.CrosspromoData.TextData.Title",
											"data":  "$$crosspromo.CrosspromoData.CrosspromoFiles",
										},
									},
								},
								"Banner": bson.M{
									"$let": bson.M{
										"vars": bson.M{
											"crosspromo": bson.M{
												"$arrayElemAt": bson.A{
													bson.M{
														"$filter": bson.M{
															"input": "$AppDynamicData.AdWaterfall.Banner",
															"as":    "item",
															"cond":  bson.M{"$eq": bson.A{"$$item.Network", "Crosspromo"}},
														},
													},
													0,
												},
											},
										},
										"in": bson.M{
											"title": "$$crosspromo.CrosspromoData.TextData.Title",
											"data":  "$$crosspromo.CrosspromoData.CrosspromoFiles",
										},
									},
								},
								"Interstitial": bson.M{
									"$let": bson.M{
										"vars": bson.M{
											"crosspromo": bson.M{
												"$arrayElemAt": bson.A{
													bson.M{
														"$filter": bson.M{
															"input": "$AppDynamicData.AdWaterfall.Interstitial",
															"as":    "item",
															"cond":  bson.M{"$eq": bson.A{"$$item.Network", "Crosspromo"}},
														},
													},
													0,
												},
											},
										},
										"in": bson.M{
											"title": "$$crosspromo.CrosspromoData.TextData.Title",
											"data":  "$$crosspromo.CrosspromoData.CrosspromoFiles",
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			"$unwind": "$platforms",
		},
		{
			"$group": bson.M{
				"_id": "$localizationCode",
				"platforms": bson.M{
					"$push": "$platforms",
				},
			},
		},
		{
			"$project": bson.M{
				"localizationCode": "$_id",
				"platforms":        1,
				"_id":              0,
			},
		},
	}
}

type server struct {
	pb.UnimplementedAssetsServiceServer
	mongoClient *mongo.Client
	mongoColl   *mongo.Collection
}

// Типы для распаковки MongoDB результата
type CrosspromoFile struct {
	AdType   string `bson:"AdType"`
	AdFormat string `bson:"AdFormat"`
	FileSize int    `bson:"FileSize"`
	Url      string `bson:"Url"`
}

type AdTypeData struct {
	Title string           `bson:"title"`
	Data  []CrosspromoFile `bson:"data"`
}

type AdTypeMap struct {
	Rewarded     AdTypeData `bson:"Rewarded"`
	Banner       AdTypeData `bson:"Banner"`
	Interstitial AdTypeData `bson:"Interstitial"`
}

type Platform struct {
	Name   string    `bson:"name"`
	AdType AdTypeMap `bson:"AdType"`
}

type AggregationResult struct {
	LocalizationCode string     `bson:"localizationCode"`
	Platforms        []Platform `bson:"platforms"`
}

func transformToProto(results []AggregationResult) *pb.GetAssetsResponse {
	var response pb.GetAssetsResponse

	for _, result := range results {
		var locPb pb.Localization
		locPb.LocalizationCode = result.LocalizationCode

		for _, platform := range result.Platforms {
			var platformPb pb.Platform
			platformPb.Name = platform.Name

			// Создаем map для AdType
			platformPb.AdType = make(map[string]*pb.AdTypeData)

			// Преобразуем Rewarded
			platformPb.AdType["Rewarded"] = &pb.AdTypeData{
				Title: platform.AdType.Rewarded.Title,
				Data:  transformCrosspromoFiles(platform.AdType.Rewarded.Data),
			}

			// Преобразуем Banner
			platformPb.AdType["Banner"] = &pb.AdTypeData{
				Title: platform.AdType.Banner.Title,
				Data:  transformCrosspromoFiles(platform.AdType.Banner.Data),
			}

			// Преобразуем Interstitial
			platformPb.AdType["Interstitial"] = &pb.AdTypeData{
				Title: platform.AdType.Interstitial.Title,
				Data:  transformCrosspromoFiles(platform.AdType.Interstitial.Data),
			}

			locPb.Platforms = append(locPb.Platforms, &platformPb)
		}

		response.Localizations = append(response.Localizations, &locPb)
	}

	return &response
}

func transformCrosspromoFiles(files []CrosspromoFile) []*pb.CrosspromoFile {
	var result []*pb.CrosspromoFile
	for _, file := range files {
		result = append(result, &pb.CrosspromoFile{
			AdType:   file.AdType,
			AdFormat: file.AdFormat,
			FileSize: int32(file.FileSize),
			Url:      file.Url,
		})
	}
	return result
}

func (s *server) GetAssets(ctx context.Context, req *pb.GetAssetsRequest) (*pb.GetAssetsResponse, error) {
	pipeline := []bson.M{}

	// Добавляем фильтрацию по query параметрам
	if len(req.Filters) > 0 {
		matchObj := bson.M{}
		for key, val := range req.Filters {
			if val != "" {
				// Поддерживаем различные поля для фильтрации
				fieldName := key
				switch strings.ToLower(key) {
				case "storecode":
					fieldName = "Cluster.StoreCode"
				case "localizationcode":
					fieldName = "Cluster.LocalizationCode"
				case "platform":
					fieldName = "Cluster.Platform"
				case "appid":
					fieldName = "Cluster.AppId"
				default:
					fieldName = "Cluster." + key
				}

				matchObj[fieldName] = bson.M{
					"$regex":   val,
					"$options": "i",
				}
			}
		}
		if len(matchObj) > 0 {
			pipeline = append(pipeline, bson.M{"$match": matchObj})
		}
	}

	pipeline = append(pipeline, getAggStages()...)

	cursor, err := s.mongoColl.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []AggregationResult
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	return transformToProto(results), nil
}

func enableCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Разрешаем запросы с любого origin (можно указать конкретные домены)
		w.Header().Set("Access-Control-Allow-Origin", "*")

		// Разрешаем методы
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")

		// Разрешаем заголовки
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		// Обрабатываем preflight OPTIONS запросы
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Передаем управление следующему обработчику
		next(w, r)
	}
}

// HTTP handler для обработки query параметров
func httpHandler(srv *server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Извлекаем query параметры
		queryParams := r.URL.Query()
		filters := make(map[string]string)

		// Поддерживаемые параметры фильтрации
		supportedParams := []string{"StoreCode", "LocalizationCode", "Platform", "AppId"}

		for _, param := range supportedParams {
			if value := queryParams.Get(param); value != "" {
				filters[param] = value
			}
		}

		// Также поддерживаем любые другие параметры, начинающиеся с "filter."
		for key, values := range queryParams {
			if strings.HasPrefix(key, "filter.") && len(values) > 0 {
				filterKey := strings.TrimPrefix(key, "filter.")
				filters[filterKey] = values[0]
			}
		}

		// Создаем gRPC запрос
		grpcReq := &pb.GetAssetsRequest{Filters: filters}

		// Вызываем gRPC метод
		response, err := srv.GetAssets(r.Context(), grpcReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Преобразуем protobuf ответ в JSON
		jsonBytes, err := protojson.MarshalOptions{
			UseProtoNames:   true, // Использует оригинальные имена полей из .proto файла
			EmitUnpopulated: true, // Включает поля с нулевыми значениями
		}.Marshal(response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Парсим JSON чтобы извлечь массив localizations
		var jsonResponse map[string]interface{}
		if err := json.Unmarshal(jsonBytes, &jsonResponse); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Извлекаем массив localizations
		localizations, exists := jsonResponse["localizations"]
		if !exists {
			http.Error(w, "localizations field not found in response", http.StatusInternalServerError)
			return
		}

		// Преобразуем обратно в JSON (теперь это просто массив)
		finalJson, err := json.Marshal(localizations)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(finalJson)
	}
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	mongoURI := os.Getenv("CONNECT_URL")
	if mongoURI == "" {
		log.Fatal("Environment variable CONNECT_URL is required")
	}
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("Mongo connect error: %v", err)
	}
	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		log.Fatalf("Mongo ping error: %v", err)
	}
	log.Println("Connected to MongoDB")

	coll := client.Database("crosspromo_assets").Collection("appDynamicData")

	grpcServer := grpc.NewServer()
	srv := &server{mongoClient: client, mongoColl: coll}
	pb.RegisterAssetsServiceServer(grpcServer, srv)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal("Failed to listen:", err)
	}

	go func() {
		log.Println("Starting gRPC server on :50051")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	// Создаем HTTP handler с поддержкой query параметров
	mux := http.NewServeMux()
	mux.HandleFunc("/api/assets/getAssets", enableCORS(httpHandler(srv)))

	log.Println("Starting HTTP server on :8080")
	err = http.ListenAndServe(":8080", mux)
	if err != nil {
		log.Fatal(err)
	}
}
