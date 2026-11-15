// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build ignore

// make-invoice-pdf writes a one-page invoice PDF with made-up content, the
// input for recording the mistral-ocr blueprint. The vendor, customer,
// addresses and numbers are fictional (postcode 000 00, organisation
// numbers of zeros), and the page says so. The output is the same bytes on
// every run, so a recording made from it can be replayed from a fresh copy.
//
//	go run internal/blueprinttest/testdata/make-invoice-pdf.go -o invoice-0001.pdf
//
// It goes into the private recordings, as
// $BLUEPRINT_TESTS_DIR/mistral-ocr/data/invoices/invoice-0001.pdf, never
// into this repository's blueprints/.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

// line is one text line at x, y (points from the bottom left), in size.
type line struct {
	x, y float64
	size float64
	bold bool
	text string
}

func invoice() []line {
	var ls []line
	add := func(x, y, size float64, bold bool, text string) {
		ls = append(ls, line{x, y, size, bold, text})
	}
	add(50, 780, 9, false, "SAMPLE DOCUMENT - fictional company, people and numbers, for software testing only")
	add(50, 740, 20, true, "INVOICE")
	add(50, 710, 11, true, "Blåklint Testverkstad AB")
	add(50, 696, 10, false, "Exempelvägen 1")
	add(50, 682, 10, false, "000 00 Påhittby, Sweden")
	add(50, 668, 10, false, "Org. no 000000-0000   VAT SE000000000001")

	add(360, 710, 10, true, "Invoice number")
	add(470, 710, 10, false, "TEST-2026-0001")
	add(360, 696, 10, true, "Invoice date")
	add(470, 696, 10, false, "2026-10-01")
	add(360, 682, 10, true, "Due date")
	add(470, 682, 10, false, "2026-10-31")
	add(360, 668, 10, true, "Currency")
	add(470, 668, 10, false, "SEK")

	add(50, 630, 10, true, "Bill to")
	add(50, 616, 10, false, "Exempelkunden Handel AB")
	add(50, 602, 10, false, "Testgatan 2")
	add(50, 588, 10, false, "000 00 Låtsasstad, Sweden")

	add(50, 545, 10, true, "Description")
	add(330, 545, 10, true, "Qty")
	add(390, 545, 10, true, "Unit price")
	add(490, 545, 10, true, "Amount")
	items := []struct {
		desc          string
		qty           string
		price, amount string
	}{
		{"Test widget, blue", "4", "125.00", "500.00"},
		{"Calibration service (hours)", "2.5", "640.00", "1,600.00"},
		{"Shipping, sample parcel", "1", "89.00", "89.00"},
	}
	y := 527.0
	for _, it := range items {
		add(50, y, 10, false, it.desc)
		add(330, y, 10, false, it.qty)
		add(390, y, 10, false, it.price)
		add(490, y, 10, false, it.amount)
		y -= 18
	}

	add(390, 455, 10, false, "Subtotal")
	add(490, 455, 10, false, "2,189.00")
	add(390, 437, 10, false, "VAT 25%")
	add(490, 437, 10, false, "547.25")
	add(390, 415, 11, true, "Total SEK")
	add(490, 415, 11, true, "2,736.25")

	add(50, 360, 9, false, "Payment: fictional, do not pay. Reference TEST-2026-0001.")
	return ls
}

// pdfString escapes s as a PDF literal string in WinAnsiEncoding, which
// covers the Swedish letters used above.
func pdfString(s string) string {
	var b strings.Builder
	b.WriteByte('(')
	for _, r := range s {
		switch {
		case r == '(' || r == ')' || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r < 0x80:
			b.WriteRune(r)
		case r <= 0xff:
			fmt.Fprintf(&b, "\\%03o", r)
		default:
			log.Fatalf("%q: %q is outside WinAnsiEncoding", s, r)
		}
	}
	b.WriteByte(')')
	return b.String()
}

func build() []byte {
	var content bytes.Buffer
	for _, l := range invoice() {
		font := "F1"
		if l.bold {
			font = "F2"
		}
		fmt.Fprintf(&content, "BT /%s %.0f Tf %.0f %.0f Td %s Tj ET\n", font, l.size, l.x, l.y, pdfString(l.text))
	}
	objs := []string{
		"<< /Type /Catalog /Pages 2 0 R >>",
		"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
		"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Contents 4 0 R /Resources << /Font << /F1 5 0 R /F2 6 0 R >> >> >>",
		fmt.Sprintf("<< /Length %d >>\nstream\n%sendstream", content.Len(), content.String()),
		"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>",
		"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold /Encoding /WinAnsiEncoding >>",
		"<< /Title (Sample invoice TEST-2026-0001, fictional) /Producer (ondatrasql make-invoice-pdf) >>",
	}
	var pdf bytes.Buffer
	pdf.WriteString("%PDF-1.4\n")
	offsets := make([]int, len(objs))
	for i, o := range objs {
		offsets[i] = pdf.Len()
		fmt.Fprintf(&pdf, "%d 0 obj\n%s\nendobj\n", i+1, o)
	}
	xref := pdf.Len()
	fmt.Fprintf(&pdf, "xref\n0 %d\n0000000000 65535 f \n", len(objs)+1)
	for _, off := range offsets {
		fmt.Fprintf(&pdf, "%010d 00000 n \n", off)
	}
	fmt.Fprintf(&pdf, "trailer\n<< /Size %d /Root 1 0 R /Info %d 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(objs)+1, len(objs), xref)
	return pdf.Bytes()
}

func main() {
	out := flag.String("o", "invoice-0001.pdf", "the PDF to write")
	flag.Parse()
	if err := os.WriteFile(*out, build(), 0o644); err != nil {
		log.Fatal(err)
	}
}
