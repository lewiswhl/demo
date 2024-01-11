function get_ds() {
    const dashboard = tableau.extensions.dashboardContent.dashboard;
    dashboard.worksheets.forEach(function (worksheet) {
        worksheet.getDataSourcesAsync().then(function (datasources) {
        datasources.forEach(function (datasource) {
            const original_text = $('#topline').text();
            const new_text = (original_text + 'ds_id:' + datasource.id + ',ds_name:' + datasource.name);
            $('#topline').text(new_text);
        });
        });
    });
    };

function getDistinctDataSources() {
    return new Promise((resolve, reject) => {
        const dashboard = tableau.extensions.dashboardContent.dashboard;
        const distinctDataSources = [];
    
        dashboard.worksheets.forEach(function (worksheet) {
        worksheet.getDataSourcesAsync().then(function (dataSources) {
            dataSources.forEach(function (dataSource) {
            if (!distinctDataSources.includes(dataSource)) {
                distinctDataSources.push(dataSource);
            }
            });
        });
        });
    
        // Wait for all Promises to resolve before returning the results
        Promise.all(dashboard.worksheets.map(worksheet => worksheet.getDataSourcesAsync()))
        .then(datasources => {
            const flattenedDataSources = datasources.flat();
            const distinctDataSources = [...new Set(flattenedDataSources)];
            resolve(distinctDataSources);
        })
        .catch(error => {
            reject(error);
        });
    });
    }