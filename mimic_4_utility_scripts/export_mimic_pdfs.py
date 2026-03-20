import csv
import os
import re
import sys
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.pdfgen import canvas
from xml.sax.saxutils import escape
from datetime import datetime

OUTPUT_DIR = "mimic_discharge_pdfs"
LOG_PATH = "pdf_generation.log"

# remove control chars that are illegal in XML/HTML except newline and tab
INVALID_CTRL_RE = re.compile(
    r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]"  # exclude \t (\x09), \n (\x0A), \r (\x0D) if you like
)


def sanitize_text_for_paragraph(raw_text: str) -> str:
    if raw_text is None:
        return ""
    # Remove problematic control chars
    cleaned = INVALID_CTRL_RE.sub("", raw_text)
    # Escape HTML special chars so ReportLab paragraph parser won't choke
    escaped = escape(cleaned)
    # Now turn newlines into real <br/> tags for line breaks in Paragraph
    with_breaks = escaped.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "<br/>")
    return with_breaks


def safe_filename(name: str) -> str:
    # Make a filesystem-safe filename (basic)
    name = str(name)
    name = re.sub(r"[^\w\-_\. ]", "_", name)
    return name


def write_plain_text_pdf(pdf_path: str, text: str):
    """
    Fallback PDF writer that writes raw lines using canvas (no HTML parsing).
    This is more robust but less pretty.
    """
    c = canvas.Canvas(pdf_path, pagesize=A4)
    width, height = A4
    margin_x = 40
    margin_top = 40
    max_width = width - 2 * margin_x
    # Use a textobject for multiline text
    textobj = c.beginText(margin_x, height - margin_top)
    textobj.setFont("Helvetica", 10)
    # naive wrap: split on newline then wrap by approx chars per line
    # estimate chars per line based on font size (very rough)
    approx_char_per_line = int(max_width / 6.5)
    for paragraph in text.splitlines():
        if not paragraph:
            textobj.textLine("")  # keep blank lines
            continue
        start = 0
        while start < len(paragraph):
            chunk = paragraph[start : start + approx_char_per_line]
            textobj.textLine(chunk)
            start += approx_char_per_line
        # after each original newline, continue
    c.drawText(textobj)
    c.showPage()
    c.save()


def log(msg: str):
    now = datetime.utcnow().isoformat()
    line = f"{now} - {msg}\n"
    with open(LOG_PATH, "a", encoding="utf8") as f:
        f.write(line)
    print(line, end="")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    CSV_INPUT_PATH = sys.argv[1] if len(sys.argv) > 1 else "mimic_discharge_notes.csv"
    count = 0
    try:
        with open(CSV_INPUT_PATH, "r", encoding="utf8") as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip header row
            for row in reader:
                note_id = row[0]
                subject_id = row[1]
                hadm_id = row[2]
                note_type = row[3]
                note_seq = row[4]
                charttime = row[5]
                storetime = row[6]
                raw_text = row[7]

                styles = getSampleStyleSheet()
                text_style = styles["Normal"]
                try:
                    subject_dir = os.path.join(OUTPUT_DIR, str(subject_id))
                    os.makedirs(subject_dir, exist_ok=True)

                    safe_note_seq = safe_filename(note_seq)
                    pdf_path = os.path.join(subject_dir, f"{safe_note_seq}.pdf")

                    # prepare text for Paragraph
                    para_text = sanitize_text_for_paragraph(raw_text)

                    # Create PDF using Paragraph (HTML-lite)
                    doc = SimpleDocTemplate(pdf_path, pagesize=A4)
                    story = [Paragraph(para_text or "", text_style)]
                    doc.build(story)
                    count += 1

                except Exception as e:
                    # If Paragraph fails for this row, log the error and write a plain-text PDF fallback
                    log(
                        f"ERROR writing Paragraph PDF for subject_id={subject_id} note_seq={note_seq}: {e}"
                    )
                    try:
                        fallback_pdf = os.path.join(
                            subject_dir, f"{safe_note_seq}_fallback.pdf"
                        )
                        # Use original raw text (not escaped) for fallback so the content is preserved
                        write_plain_text_pdf(fallback_pdf, raw_text or "")
                        count += 1
                        log(
                            f"WROTE fallback plain-text PDF for subject_id={subject_id} note_seq={note_seq} -> {fallback_pdf}"
                        )
                    except Exception as e2:
                        log(
                            f"CRITICAL: failed fallback for subject_id={subject_id} note_seq={note_seq}: {e2}"
                        )

                log(f"Done. Generated/processed {count} rows.")

    except Exception as conn_e:
        log(f"Critical error occurred: {conn_e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
