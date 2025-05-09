from fpdf import FPDF
import json

class PDFReport(FPDF):
    def __init__(self, dataset_name, total_score, logo_path=None):
        super().__init__()
        self.dataset_name = dataset_name
        self.total_score = total_score
        self.logo_path = logo_path
        self.set_auto_page_break(auto=True, margin=15)
        self.add_page()
        self.set_font("Arial", size=10)

    def header(self):
        if self.logo_path:
            self.image(self.logo_path, 10, 8, 20)  # Logo at top-left, width = 20

        self.set_font("Arial", 'B', 14)
        self.set_xy(5, 10)
        self.cell(0, 10, 'Data Readiness Report', ln=True, align='C')

        self.set_font("Arial", '', 11)
        self.set_xy(10,25)
        self.cell(0, 10, f"Dataset: {self.dataset_name}", ln=True, align='L')
        self.set_xy(160,25)
        self.set_font("Arial", 'B', 12)
        self.cell(0, 10, f"Overall Score: {self.total_score:.2f}%", ln=True, align='L')

        # self.ln(5)

    def render_table(self, data):
        col_widths = [20, 60, 90, 13, 12]
        headers = ['Test ID', 'Test Description', 'Data Readiness Check Summary Notes', 'Score', 'Max']

        # Table headers
        self.set_font("Arial", 'B', 9)
        for i, header in enumerate(headers):
            self.cell(col_widths[i], 10, header, border=1)
        self.ln()

        self.set_font("Arial", '', 9)
        for section in data:
            # Bucket heading
            self.set_font("Arial", 'B', 10)
            self.cell(sum(col_widths), 8, f"{section['bucket']} ({section['weight']}%)", ln=True, border=1)
            self.set_font("Arial", '', 9)

            for test in section['tests']:
                self.cell(col_widths[0], 8, test['id'], border=1)
                self.cell(col_widths[1], 8, test['title'], border=1)

                # Summary Notes (multi-line)
                x, y = self.get_x(), self.get_y()
                self.multi_cell(col_widths[2], 8, test['note'], border=1)
                height = self.get_y() - y
                self.set_xy(x + col_widths[2], y)

                self.cell(col_widths[3], height, f"{test['score']:.2f}", border=1, align='C')
                self.cell(col_widths[4], height, f"{test['max_score']:.0f}", border=1, align='C')
                self.ln()

def generate_pdf_from_json(json_path, output_path, dataset_name, total_score, logo_path=None):
    with open(json_path, "r") as f:
        data = json.load(f)

    pdf = PDFReport(dataset_name, total_score, logo_path)
    pdf.render_table(data)
    pdf.output(output_path)

# raw_report_path = "outputReports/readiness_report.json"
# json_input = "outputReports/final_readiness_report.json"
# pdf_output = "outputReports/data_readiness_report.pdf"
# dataset_name = "Hyderabad PCA 2011 Census"
# with open(raw_report_path, "r") as f:
#     data = json.load(f)
# total_score = data["total_score"]
# logo_path = "plots/pretty/TGDEX_Logo Unit_Green.png"  # Set this to None if not needed

