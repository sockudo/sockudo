# Adds the RideWidget Live Activity extension to Flutter's Runner project.
# Run: GEM_HOME=/opt/homebrew/Cellar/cocoapods/1.17.0/libexec ruby add_widget_target.rb
require 'xcodeproj'
project = Xcodeproj::Project.open('Runner.xcodeproj')
runner = project.targets.find { |t| t.name == 'Runner' }
abort 'RideWidget already present' if project.targets.any? { |t| t.name == 'RideWidget' }

widget = project.new_target(:app_extension, 'RideWidget', :ios, '18.0')
widget_group = project.main_group.new_group('RideWidget', 'RideWidget')
shared_group = project.main_group.new_group('Shared', 'Shared')
attrs = shared_group.new_file('RideAttributes.swift')
%w[RideWidgetBundle.swift RideLiveActivity.swift].each do |name|
  widget.add_file_references([widget_group.new_file(name)])
end
widget.add_file_references([attrs])
runner.add_file_references([attrs])
runner_group = project.main_group['Runner']
runner.add_file_references([runner_group.new_file('LiveActivityBridge.swift')])
widget_group.new_file('Info.plist')

widget.build_configurations.each do |config|
  config.build_settings['INFOPLIST_FILE'] = 'RideWidget/Info.plist'
  config.build_settings['GENERATE_INFOPLIST_FILE'] = 'NO'
  config.build_settings['PRODUCT_BUNDLE_IDENTIFIER'] = 'com.sockudo.ridesFlutter.RideWidget'
  config.build_settings['PRODUCT_NAME'] = '$(TARGET_NAME)'
  config.build_settings['SWIFT_VERSION'] = '5.0'
  config.build_settings['SKIP_INSTALL'] = 'YES'
  config.build_settings['CURRENT_PROJECT_VERSION'] = '1'
  config.build_settings['MARKETING_VERSION'] = '1.0'
  config.build_settings['CODE_SIGN_STYLE'] = 'Automatic'
  config.build_settings['TARGETED_DEVICE_FAMILY'] = '1'
  config.build_settings['LD_RUNPATH_SEARCH_PATHS'] = ['$(inherited)', '@executable_path/Frameworks', '@executable_path/../../Frameworks']
end
runner.build_configurations.each do |config|
  config.build_settings['IPHONEOS_DEPLOYMENT_TARGET'] = '18.0'
end
project.build_configurations.each do |config|
  config.build_settings['IPHONEOS_DEPLOYMENT_TARGET'] = '18.0'
end

runner.add_dependency(widget)
embed = runner.new_copy_files_build_phase('Embed Foundation Extensions')
embed.dst_subfolder_spec = '13'
embed.add_file_reference(widget.product_reference).settings = { 'ATTRIBUTES' => ['RemoveHeadersOnCopy'] }
project.save
puts 'RideWidget target added'
