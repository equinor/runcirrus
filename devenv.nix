{ ... }:

{
  languages.rust.enable = true;

  env.KARSKSAL_ROOT = "/opt/karsk/project";
  env.RUNKARSK_EXECUTABLE = "cirrus";
  env.RUNKARSK_SETUP_COMMAND = "/bin/true";
}
