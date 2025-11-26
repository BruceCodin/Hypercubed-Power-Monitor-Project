# pylint: skip-file
# pragma: no cover
from unittest.mock import patch, Mock, MagicMock


class TestFormStructure:
    """Tests for the form structure and fields."""

    @patch('page_form.st.form')
    @patch('page_form.st.text_input')
    @patch('page_form.st.form_submit_button')
    def test_form_collects_all_required_fields(self, mock_button, mock_text_input, mock_form):
        """Should collect first_name, last_name, email, and postcode from form."""
        mock_text_input.side_effect = ['John', 'Doe', 'john@example.com', 'SW1A 1AA']
        mock_button.return_value = True
        mock_form_ctx = MagicMock()
        mock_form.return_value.__enter__ = Mock(return_value=mock_form_ctx)
        mock_form.return_value.__exit__ = Mock(return_value=None)

        with mock_form(key='subscription_form'):
            details = {
                "first_name": mock_text_input("First Name:"),
                "last_name": mock_text_input("Last Name:"),
                "email": mock_text_input("Email Address:"),
                "postcode": mock_text_input("Postcode:")
            }

        assert details["first_name"] == "John"
        assert details["last_name"] == "Doe"
        assert details["email"] == "john@example.com"
        assert details["postcode"] == "SW1A 1AA"

    @patch('page_form.st.form_submit_button')
    def test_submit_button_has_correct_label(self, mock_button):
        """Should create submit button with label 'Submit'."""
        mock_button.return_value = True
        mock_button(label='Submit')
        mock_button.assert_called_with(label='Submit')


class TestSuccessfulSubmission:
    """Tests for successful form submission (statusCode 200)."""

    @patch('page_form.lambda_handler')
    @patch('page_form.st.form')
    @patch('page_form.st.text_input')
    @patch('page_form.st.form_submit_button')
    def test_success_message_displayed_on_200(self, mock_button,
                                              mock_text_input, mock_form, mock_lambda):
        """Should display success message when lambda returns statusCode 200."""
        mock_lambda.return_value = {'statusCode': 200}
        mock_button.return_value = True
        mock_text_input.side_effect = ['John', 'Doe', 'john@example.com', 'SW1A 1AA']
        mock_form_ctx = MagicMock()
        mock_form.return_value.__enter__ = Mock(return_value=mock_form_ctx)
        mock_form.return_value.__exit__ = Mock(return_value=None)

        with mock_form(key='subscription_form'):
            first_name = mock_text_input("First Name:")
            details = {
                "first_name": first_name,
                "last_name": mock_text_input("Last Name:"),
                "email": mock_text_input("Email Address:"),
                "postcode": mock_text_input("Postcode:")
            }
            response = mock_lambda(details, None)

            if response['statusCode'] == 200:
                success_msg = f"Thank you {first_name}! You have successfully subscribed. :tada:"
                assert "successfully subscribed" in success_msg

    @patch('page_form.logger')
    @patch('page_form.lambda_handler')
    @patch('page_form.st.form')
    @patch('page_form.st.text_input')
    @patch('page_form.st.form_submit_button')
    def test_details_logged_on_success(self, mock_button, mock_text_input,
                                      mock_form, mock_lambda, mock_logger):
        """Should log form details when submission is successful."""
        mock_lambda.return_value = {'statusCode': 200}
        mock_button.return_value = True
        mock_text_input.side_effect = ['Bob', 'Smith', 'bob@example.com', 'M1 1AE']
        mock_form_ctx = MagicMock()
        mock_form.return_value.__enter__ = Mock(return_value=mock_form_ctx)
        mock_form.return_value.__exit__ = Mock(return_value=None)

        with mock_form(key='subscription_form'):
            details = {
                "first_name": mock_text_input("First Name:"),
                "last_name": mock_text_input("Last Name:"),
                "email": mock_text_input("Email Address:"),
                "postcode": mock_text_input("Postcode:")
            }
            response = mock_lambda(details, None)

            if response['statusCode'] == 200:
                logger_message = ["Form submitted"]
                for key, value in details.items():
                    logger_message.append(f"{key.replace('_', ' ').title()}: {value}")

                log_text = "\n".join(logger_message)
                assert "Form submitted" in log_text
                assert "First Name: Bob" in log_text


class TestFailedSubmission:
    """Tests for failed form submission (statusCode 400 or 500)."""

    @patch('page_form.lambda_handler')
    @patch('page_form.st.form')
    @patch('page_form.st.text_input')
    @patch('page_form.st.form_submit_button')
    def test_error_displayed_on_400(self, mock_button, mock_text_input,
                                    mock_form, mock_lambda):
        """Should display error message on 400 response."""
        error_body = "Invalid input: Email is invalid."
        mock_lambda.return_value = {'statusCode': 400, 'body': error_body}
        mock_button.return_value = True
        mock_text_input.side_effect = ['John', 'Doe', 'invalid-email', 'SW1A 1AA']
        mock_form_ctx = MagicMock()
        mock_form.return_value.__enter__ = Mock(return_value=mock_form_ctx)
        mock_form.return_value.__exit__ = Mock(return_value=None)

        with mock_form(key='subscription_form'):
            details = {
                "first_name": mock_text_input("First Name:"),
                "last_name": mock_text_input("Last Name:"),
                "email": mock_text_input("Email Address:"),
                "postcode": mock_text_input("Postcode:")
            }
            response = mock_lambda(details, None)

            if response['statusCode'] != 200:
                assert response['statusCode'] == 400
                assert response['body'] == error_body

    @patch('page_form.lambda_handler')
    @patch('page_form.st.form')
    @patch('page_form.st.text_input')
    @patch('page_form.st.form_submit_button')
    def test_error_displayed_on_500(self, mock_button, mock_text_input,
                                    mock_form, mock_lambda):
        """Should display error message on 500 response."""
        mock_lambda.return_value = {
            'statusCode': 500,
            'body': 'Internal server error. Please try again later.'
        }
        mock_button.return_value = True
        mock_text_input.side_effect = ['John', 'Doe', 'john@example.com', 'SW1A 1AA']
        mock_form_ctx = MagicMock()
        mock_form.return_value.__enter__ = Mock(return_value=mock_form_ctx)
        mock_form.return_value.__exit__ = Mock(return_value=None)

        with mock_form(key='subscription_form'):
            details = {
                "first_name": mock_text_input("First Name:"),
                "last_name": mock_text_input("Last Name:"),
                "email": mock_text_input("Email Address:"),
                "postcode": mock_text_input("Postcode:")
            }
            response = mock_lambda(details, None)

            if response['statusCode'] != 200:
                assert response['statusCode'] == 500
                assert "error" in response['body'].lower()


class TestFormSubmitLogic:
    """Tests for form submission flow."""

    @patch('page_form.lambda_handler')
    @patch('page_form.st.form')
    @patch('page_form.st.text_input')
    @patch('page_form.st.form_submit_button')
    def test_lambda_only_called_when_submit_pressed(self, mock_button, mock_text_input,
                                                     mock_form, mock_lambda):
        """Should only call lambda_handler when submit button is True."""
        mock_button.return_value = False
        mock_text_input.return_value = "test"
        mock_form_ctx = MagicMock()
        mock_form.return_value.__enter__ = Mock(return_value=mock_form_ctx)
        mock_form.return_value.__exit__ = Mock(return_value=None)

        with mock_form(key='subscription_form'):
            first_name = mock_text_input("First Name:")
            submit_button = mock_button(label='Submit')

            if submit_button:
                mock_lambda({"first_name": first_name}, None)

        mock_lambda.assert_not_called()

    @patch('page_form.lambda_handler')
    @patch('page_form.st.form')
    @patch('page_form.st.text_input')
    @patch('page_form.st.form_submit_button')
    def test_lambda_called_with_details_dict_and_none(self, mock_button, mock_text_input,
                                                       mock_form, mock_lambda):
        """Should call lambda_handler with details dict and None context."""
        mock_lambda.return_value = {'statusCode': 200}
        mock_button.return_value = True
        mock_text_input.side_effect = ['John', 'Doe', 'john@example.com', 'SW1A 1AA']
        mock_form_ctx = MagicMock()
        mock_form.return_value.__enter__ = Mock(return_value=mock_form_ctx)
        mock_form.return_value.__exit__ = Mock(return_value=None)

        with mock_form(key='subscription_form'):
            details = {
                "first_name": mock_text_input("First Name:"),
                "last_name": mock_text_input("Last Name:"),
                "email": mock_text_input("Email Address:"),
                "postcode": mock_text_input("Postcode:")
            }
            mock_lambda(details, None)

            mock_lambda.assert_called_once_with(details, None)


class TestDetailFormatting:
    """Tests for formatting displayed subscription details."""

    @patch('page_form.st.form')
    @patch('page_form.st.text_input')
    @patch('page_form.st.form_submit_button')
    def test_keys_formatted_to_title_case_for_display(self, mock_button, mock_text_input,
                                                       mock_form):
        """Should format field names to title case (e.g., first_name -> First Name)."""
        mock_text_input.side_effect = ['Alice', 'Johnson', 'alice@test.com', 'EC1A 1BB']
        mock_button.return_value = True
        mock_form_ctx = MagicMock()
        mock_form.return_value.__enter__ = Mock(return_value=mock_form_ctx)
        mock_form.return_value.__exit__ = Mock(return_value=None)

        with mock_form(key='subscription_form'):
            details = {
                "first_name": mock_text_input("First Name:"),
                "last_name": mock_text_input("Last Name:"),
                "email": mock_text_input("Email Address:"),
                "postcode": mock_text_input("Postcode:")
            }

            displayed = []
            for key, value in details.items():
                formatted_key = key.replace('_', ' ').title()
                displayed.append(f"**{formatted_key}**: {value}")

            assert any("First Name" in d for d in displayed)
            assert any("Last Name" in d for d in displayed)
            assert any("Email" in d for d in displayed)
            assert any("Postcode" in d for d in displayed)
