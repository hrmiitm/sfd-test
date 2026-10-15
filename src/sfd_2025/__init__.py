"""SFD financial filing pipeline."""


def main() -> None:
    from .pipeline.run import main as pipeline_main
    pipeline_main()
