# frozen_string_literal: true

module Sockudo
  module Utils
    def validate_socket_id(socket_id)
      return if socket_id && /\A\d+\.\d+\z/.match(socket_id)

      raise Sockudo::Error, "Invalid socket ID #{socket_id.inspect}"
    end

    # Compute authentication string required as part of the user authentication
    # and subscription authorization endpoints responses.
    # Generally the authenticate method should be used in preference to this one.
    #
    # @param socket_id [String] Each Sockudo socket connection receives a
    #   unique socket_id. This is sent from sockudo.js to your server when
    #   channel authentication is required.
    # @param custom_string [String] Allows signing additional data
    # @return [String]
    #
    # @raise [Sockudo::Error] if socket_id or custom_string invalid
    #
    def _authentication_string(socket_id, string_to_sign, token, custom_string = nil)
      validate_socket_id(socket_id)

      raise Sockudo::Error, 'Custom argument must be a string' unless custom_string.nil? || custom_string.is_a?(String)

      Sockudo.logger.debug "Signing #{string_to_sign}"

      digest = OpenSSL::Digest.new('SHA256')
      signature = OpenSSL::HMAC.hexdigest(digest, token.secret, string_to_sign)

      "#{token.key}:#{signature}"
    end
  end
end
