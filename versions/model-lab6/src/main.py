import argparse

from model import ModelKMEANS
from preprocess import PreProcessor
from datasource import MsSqlDataSource


def parse_args():
    """Парсим командную строку"""
    parser = argparse.ArgumentParser(
        description="Обучение и инференс KMeans на Open Food Facts"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("preprocess", help="Преобработать данные и загрузить в БД")

    subparsers.add_parser(
        "train", help="Обучить модель и сохранить модель/скейлер/отчёт"
    )

    predict_parser = subparsers.add_parser(
        "predict", help="Применить уже обученную модель"
    )
    predict_parser.add_argument(
        "--output",
        dest="predictions_path",
        default=None,
        help="Куда сохранить предсказания (по умолчанию — model.predictions_path из конфига)",
    )
    predict_parser.add_argument(
        "--run-id",
        dest="train_run_id",
        type=int,
        default=None,
        help="run_id запуска обучения, чью модель применить (по умолчанию — последний успешный)",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    datasource = MsSqlDataSource()
    preprocessor = PreProcessor(datasource)
    model = ModelKMEANS()

    if args.command == "preprocess":
        preprocessor.run()
    elif args.command == "train":
        model.train()
    elif args.command == "predict":
        predictions_path = args.predictions_path or model.config.model.predictions_path
        model.predict(predictions_path, train_run_id=args.train_run_id)


if __name__ == "__main__":
    main()
