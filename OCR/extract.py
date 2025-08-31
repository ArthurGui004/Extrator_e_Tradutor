import io
import time
import os
import logging
import tempfile
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from PyPDF2 import PdfReader, PdfWriter
from collections import defaultdict
from markitdown import MarkItDown


_log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def page_to_bytesio(pdf_path: str, page_number: int) -> io.BytesIO | None:
    """
    Cria um PDF de 1 página em memória (BytesIO) contendo a página `page_number` (1-based)
    Retorna BytesIO pronto para leitura (pos 0) ou None se falhar.
    """
    try:
        reader = PdfReader(str(pdf_path))
        idx = page_number - 1
        if idx < 0 or idx >= len(reader.pages):
            _log.error(f"Página fora do range: {page_number} em {pdf_path}")
            return None
        writer = PdfWriter()
        writer.add_page(reader.pages[idx])
        bio = io.BytesIO()
        writer.write(bio)
        bio.seek(0)
        return bio
    except Exception as e:
        _log.exception(f"Erro gerando BytesIO para {pdf_path} page {page_number}: {e}")
        return None


def process_page(pdf_path: str, page_number: int, outdir: str, enable_plugins: bool = False) -> dict:
    """
    Salva o resultado em results/MD/<pdfstem>/<pdfstem>_page_<N>.md
    Retorna dict com status e metadados.
    """
    start = time.time()
    path = Path(pdf_path)
    outdir_p = Path(outdir)
    outdir_p.mkdir(parents=True, exist_ok=True)
    page_id = f"{path.stem}_page_{page_number}"
    md_path = outdir_p / f"{page_id}.md"

    try:
        bio = page_to_bytesio(pdf_path, page_number)
        if bio is None:
            return {"status": "err", "msg": "failed to create single-page pdf", "pdf": pdf_path, "page": page_number}

        md = MarkItDown(enable_plugins=enable_plugins)

        try:
            result = md.convert(bio)
            text = getattr(result, "text_content", None)
        except Exception as e_stream:
            _log.debug(f"MarkItDown.convert(stream) falhou: {e_stream}; tentando fallback para arquivo temporário.")
            text = None

        # fallback: escrever um arquivo temporário e chamar md.convert(path)
        if not text:
            try:
                with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmpf:
                    tmp_path = Path(tmpf.name)
                    tmpf.write(bio.getvalue())
                result = md.convert(str(tmp_path))
                text = getattr(result, "text_content", None)
            finally:
                try:
                    if 'tmp_path' in locals() and tmp_path.exists():
                        tmp_path.unlink()
                except Exception:
                    pass

        if not text:
            return {"status": "err", "msg": "MarkItDown não retornou texto", "pdf": pdf_path, "page": page_number}

        # salvar markdown
        with md_path.open("w", encoding="utf-8") as f:
            f.write(f"# {path.stem} — página {page_number}\n\n")
            f.write(text.strip())

        elapsed = time.time() - start
        return {"status": "ok", "msg": f"converted in {elapsed:.2f}s", "pdf": pdf_path, "page": page_number, "md": str(md_path)}
    except Exception as e:
        _log.exception("Erro no worker process_pdf_page_markitdown")
        return {"status": "err", "msg": str(e), "pdf": pdf_path, "page": page_number}


def export_documents(input_doc_paths: list[Path], max_workers: int | None = None, max_outstanding: int | None = None, per_pdf_limit: int = 2, enable_plugins: bool = False):
    """
    - max_workers: máximo de cpus que serão usadas
    - per_pdf_limit: quantas páginas do mesmo PDF podem estar em execução simultânea
    - max_outstanding: máximo de futures submetidos simultaneamente (proteção de memória)
    """
    start_all = time.time()
    cpu = os.cpu_count() or 1
    max_workers = max_workers or max(1, cpu - 1)
    max_outstanding = max_outstanding or max_workers * 4

    _log.info(f"Parâmetros usados: max_workers={max_workers}, max_outstanding={max_outstanding}, per_pdf_limit={per_pdf_limit}")

    pdfs = []
    for p in input_doc_paths:
        if p.suffix.lower() == ".pdf":
            try:
                reader = PdfReader(str(p))
                num_pages = len(reader.pages)
                pdfs.append({"path": str(p), "num_pages": num_pages, "next_page": 1, "outdir": str(Path("results/MD") / p.stem)})
            except Exception as e:
                _log.error(f"Falha lendo PDF {p}: {e}")

    total_pages = sum(p["num_pages"] for p in pdfs)
    _log.info(f"Total de páginas a processar: {total_pages}")

    futures = {}
    per_pdf_outstanding = defaultdict(int)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        while any(p["next_page"] <= p["num_pages"] for p in pdfs) or futures:
            # enfileirar enquanto houver espaço
            while len(futures) < max_outstanding:
                submitted = False
                for p in pdfs:
                    if p["next_page"] <= p["num_pages"] and per_pdf_outstanding[p["path"]] < per_pdf_limit:
                        pg = p["next_page"]
                        try:
                            fut = executor.submit(process_page, p["path"], pg, p["outdir"], enable_plugins)
                            futures[fut] = {"pdf": p["path"], "page": pg}
                            per_pdf_outstanding[p["path"]] += 1
                            p["next_page"] += 1
                            submitted = True
                            break
                        except Exception as e:
                            _log.exception(f"Falha ao submeter página {pg} de {p['path']}: {e}")
                            p["next_page"] += 1
                if not submitted:
                    break

            if not futures:
                time.sleep(0.05)
                continue

            done, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)
            for fut in done:
                meta = futures.pop(fut)
                res = None
                try:
                    res = fut.result()
                except Exception as e:
                    _log.exception(f"Future falhou: {e}")

                pdf_path = meta["pdf"]
                per_pdf_outstanding[pdf_path] = max(0, per_pdf_outstanding[pdf_path] - 1)

                if res and res.get("status") == "ok":
                    _log.info(f"OK: {res.get('pdf')} page {res.get('page')} -> {res.get('md')} ({res.get('msg')})")
                else:
                    _log.error(f"ERR: {res} for {meta}")

    total_elapsed = time.time() - start_all
    _log.info(f"Todos processados em {total_elapsed:.2f}s.")


if __name__ == "__main__":
    DATA_DIR = Path("Data")
    DATA_DIR.mkdir(exist_ok=True)
    input_doc_paths = [path for path in DATA_DIR.rglob("*.pdf")]

    # Ajuste conforme sua máquina:
    export_documents(input_doc_paths, max_workers=4, max_outstanding=420, per_pdf_limit=50)