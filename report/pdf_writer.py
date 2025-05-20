from fpdf import FPDF
import json
import datetime

class PDFReport(FPDF):
    def __init__(self, dataset_name, total_score, logo_path=None):
        """
        Constructor for PDFReport

        Parameters
        ----------
        dataset_name : str
            Name of the dataset
        total_score : float
            Total score of the dataset
        logo_path : str, optional
            Path to the logo to be displayed on the top left of the report, by default None
        """
        super().__init__()
        self.dataset_name = dataset_name
        self.total_score = total_score
        self.logo_path = logo_path
        self.set_auto_page_break(auto=True, margin=15)
        self.add_page()
        self.set_font("Helvetica", size=10)

    def header(self):
        if self.logo_path:
            self.image(self.logo_path, 10, 5, 25)  # Logo at top-left, width = 20

        self.set_font("Helvetica", 'B', 16)
        self.set_xy(15, 10)
        self.cell(0, 10, 'Data Readiness Report', ln=True, align='C')

        self.set_font("Helvetica", '', 12)
        self.set_xy(9, 30)
        self.cell(0, 10, f"Dataset Name: {self.dataset_name}", ln=True, align='L')

        self.set_font("Helvetica", '', 12)
        self.set_xy(9, 35)
        self.cell(0, 10, f"Report generated on: {datetime.datetime.now().strftime('%d-%b-%Y')}", ln=True, align='L')

        self.set_font("Helvetica", 'B', 12)
        self.set_xy(160, 35)
        self.cell(0, 10, f"Overall Score: {self.total_score:.2f}%", ln=True, align='L')

        # self.ln(5)

    def render_table(self, data):
        """
        Render the table of data readiness checks from the given data.

        Parameters
        ----------
        data : list
            List of sections, each containing a bucket name, weight, and a list of tests.
            Each test is a dictionary with keys 'id', 'title', 'note', 'score', and 'max_score'.
        """
        col_widths = [15, 40, 115, 13, 12]
        headers = [' Test ID', '       Test Description', '                                              Summary of Findings', ' Score', ' Max']

        # Table headers
        self.set_font("Helvetica", 'B', 9)
        for i, header in enumerate(headers):
            self.cell(col_widths[i], 10, header, border=1)
        self.ln()

        self.set_font("Helvetica", '', 9)
        for section in data:
            # Bucket heading
            self.set_fill_color(200, 240, 200)  # Light green
            self.set_text_color(0, 0, 0)  # Black text
            self.set_font("Helvetica", 'B', 10)
            self.cell(sum(col_widths), 8, f"{section['bucket']} ({section['weight']}%)", ln=True, fill=True, border=1)
            self.set_fill_color(255, 255, 255)  # White background
            self.set_text_color(0, 0, 0)  # Black text
            self.set_font("Helvetica", '', 9)

            # Render each test
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

            # Total score for each bucket
            self.set_font("Helvetica", 'B', 10)
            self.set_fill_color(200, 211, 211)  # Light gray
            self.cell(col_widths[0]+col_widths[1]+col_widths[2], 8, f"Subtotal", border=1, fill=True)
            self.cell(col_widths[3], 8, f"{sum(test['score'] for test in section['tests']):.2f}", border=1, align='C', fill=True)
            self.cell(col_widths[4], 8, f"{sum(test['max_score'] for test in section['tests']):.0f}", border=1, align='C', fill=True)
            self.ln()

def generate_pdf_from_json(json_path, output_path, dataset_name, total_score, logo_path=None):
    """
    Generates a PDF report from a JSON file containing readiness data.

    Parameters
    ----------
    json_path : str
        The path to the JSON file containing the readiness data.
    output_path : str
        The path where the generated PDF report will be saved.
    dataset_name : str
        The name of the dataset for which the report is generated.
    total_score : float
        The overall readiness score of the dataset.
    logo_path : str, optional
        The path to the logo image to be included in the report header (default is None).

    Returns
    -------
    None
    """

    with open(json_path, "r") as f:
        data = json.load(f)

    pdf = PDFReport(dataset_name, total_score, logo_path)
    pdf.render_table(data)
    pdf.output(output_path)
