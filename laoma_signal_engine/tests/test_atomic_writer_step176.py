from pathlib import Path

from laoma_signal_engine.core import atomic_writer


def test_step1_76_write_file_atomic_retries_permission_error(tmp_path: Path, monkeypatch) -> None:
    out = tmp_path / "DATA" / "market" / "futures_light_snapshot.json"
    calls: list[tuple[Path, Path]] = []
    real_replace = atomic_writer.os.replace

    def flaky_replace(src: Path, dst: Path) -> None:
        calls.append((Path(src), Path(dst)))
        if len(calls) == 1:
            raise PermissionError("simulated WinError 5")
        real_replace(src, dst)

    monkeypatch.setattr(atomic_writer.os, "name", "nt")
    monkeypatch.setattr(atomic_writer.os, "replace", flaky_replace)
    monkeypatch.setattr(atomic_writer.time, "sleep", lambda _seconds: None)

    atomic_writer.write_file_atomic(out, b'{"ok":true}\n', windows_retries=3)

    assert out.read_text(encoding="utf-8") == '{"ok":true}\n'
    assert len(calls) == 2
    assert calls[0][0].name.startswith("futures_light_snapshot.json.")
    assert not list(out.parent.glob("*.tmp"))
