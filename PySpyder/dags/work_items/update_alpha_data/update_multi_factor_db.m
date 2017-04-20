function update_multi_factor_db(refDate)
    delete('update_multi_factor_db.log');
    diary('update_multi_factor_db.log')
    diary on
    
    current_folder = pwd;
    project_path = 'D:\dev\svn\lishun\DataUpdate';

    cd(project_path);
    addpath(genpath(project_path));

    try
        updateMultiFactorDB(refDate);
    catch exception
        disp(getReport(exception));
        diary off
        cd(current_folder);
        exit;
    end

    diary off
    cd(current_folder);
    exit
end
