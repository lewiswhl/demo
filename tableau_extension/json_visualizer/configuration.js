(function(){

function closeDialog () {
    tableau.extensions.settings.saveAsync().then((newSavedSettings) => {
    tableau.extensions.ui.closeDialog(' ');
    });
    };

function add_option(id, display_text, _value='default_value') {
    const selectTag = $(id);
    const newOption = $('<option>', {'text': display_text, 'value':_value });
    selectTag.append(newOption);
    };

function update_field_dropdown(){
    $("#dropdown_worksheet").change(function() {
        var selectedText = $(this).find(':selected').text(); 
        var worksheet = tableau.extensions.dashboardContent.dashboard.worksheets.find(w => w.name === selectedText);
        worksheet.getSummaryDataAsync({maxRows : 1 }).then(
            (datatable)=>{
                $('#dropdown_field').empty();
                let ar_col_name = datatable.columns.map(i => i.fieldName);
                ar_col_name.forEach(i => add_option('#dropdown_field',i) );
            });
      });
    };

function select_json_field(){
    $("#dropdown_field").change(function() {
        var selectedText = $(this).find(':selected').text(); 
        tableau.extensions.settings.set('selected_sheet', $('#dropdown_worksheet').find(':selected').text());
        tableau.extensions.settings.set('selected_field', selectedText);
      });
    };

function select_json_crack_option(){
    $("#dropdown_direction").change(function() {
        var selectedText = $(this).find(':selected').text(); 
        tableau.extensions.settings.set('direction', $('#dropdown_direction').find(':selected').text());
      });
    $("#dropdown_theme").change(function() {
        var selectedText = $(this).find(':selected').text(); 
        tableau.extensions.settings.set('theme', $('#dropdown_theme').find(':selected').text());
      });
    };

function main(){
    tableau.extensions.initializeDialogAsync().then( 
        (payload_from_main_to_configuration)=>{
            //event listeners
            $('#subbutton').click(closeDialog);
            update_field_dropdown();
            select_json_field();
            select_json_crack_option();

            const worksheets = tableau.extensions.dashboardContent.dashboard.worksheets;
            $('#dropdown_worksheet').empty();
            worksheets.forEach((ws)=>{
                    add_option('#dropdown_worksheet', ws.name);
                    const get_data_option = { ignoreSelection : false, maxRows : 1 };
                });
        })
    };
    
$(document).ready(main);
})();